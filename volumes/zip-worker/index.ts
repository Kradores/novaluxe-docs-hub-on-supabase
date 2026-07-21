import { createClient } from "@supabase/supabase-js";
import archiver from "archiver";
import { Readable } from "stream";
import fs from "node:fs";
import path from "node:path";
import { ReadableStream } from 'stream/web';
import { Transform } from "node:stream";
import { sanitize, retry, throttleAsync, sleep } from "./utils.js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const BUCKET = "documents";
const UPLOADED_BUCKET = "collection-uploaded-documents";
const TEMP_DIR = "/tmp/zip-worker";
const HEARTBEAT_INTERVAL = 30_000;

let shuttingDown = false;
let currentJobId: string | null = null;

type ZipJobStage =
  | "preparing"
  | "downloading"
  | "compressing"
  | "uploading"
  | "completed"
  | "failed";

type ZipDocument = {
  bucket: typeof BUCKET | typeof UPLOADED_BUCKET;
  path: string;
  zipPath: string;
  size: number;
};

type ZipJob = {
  id: string;
  collection_id: string;
  attempts: number;
};

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
}).schema("public");


async function heartbeat(jobId: string) {
  await supabase
    .from("zip_jobs")
    .update({
      updated_at: new Date().toISOString(),
    })
    .eq("id", jobId);
}

async function claimJob(): Promise<ZipJob | null> {
  if (shuttingDown) return null;

  const { data, error } = await supabase
    .rpc("claim_zip_job")
    .maybeSingle<ZipJob>();

  if (error) {
    console.error("Failed to claim job:", error);
    return null;
  }

  console.log(data);

  if (!data?.id) {
    console.log("No job available to claim.");
    return null;
  }

  return data;
}

async function updateProgress(
  jobId: string,
  processed: number,
  total: number
) {
  const progress =
    total === 0
      ? 0
      : Math.min(
        100,
        Math.floor((processed / total) * 100)
      );

  await supabase
    .from("zip_jobs")
    .update({
      processed_bytes: processed,
      total_bytes: total,
      progress,
      updated_at: new Date().toISOString(),
    })
    .eq("id", jobId);
}

async function failJob(jobId: string, error: unknown) {
  await supabase
    .from("zip_jobs")
    .update({
      status: "failed",
      last_error: String(error),
      updated_at: new Date().toISOString(),
    })
    .eq("id", jobId);
}

async function completeJob(jobId: string) {
  await supabase
    .from("zip_jobs")
    .update({
      status: "ready",
      stage: "completed",
      progress: 100,
      last_error: null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", jobId);
}

async function appendDocument(
  archive: archiver.Archiver,
  document: ZipDocument,
  onProgress: (bytes: number) => void
): Promise<void> {
  const response = await fetch(
    `${SUPABASE_URL}/storage/v1/object/${document.bucket}/${document.path}`,
    {
      headers: {
        Authorization: `Bearer ${SERVICE_ROLE_KEY}`,
      },
    }
  );

  if (!response.ok) {
    throw new Error(
      `Failed downloading '${document.path}'. Status: ${response.status}`
    );
  }

  if (!response.body) {
    throw new Error(
      `Storage returned an empty stream for '${document.path}'.`
    );
  }

  const nodeStream = Readable.fromWeb(response.body as ReadableStream<Uint8Array>);

  const progressStream = new Transform({
    transform(chunk, _, callback) {
      onProgress(chunk.length);
      callback(null, chunk);
    },
  });

  archive.append(
    nodeStream.pipe(progressStream),
    {
      name: document.zipPath,
    }
  );
}

async function loadCollectionRelationships(collectionId: string) {
  const { data: collection, error: collectionError } = await supabase
    .from("document_collections")
    .select(`
    id,
    collection_company_documents(company_document_id),
    collection_worker_document_types(worker_document_type_id),
    collection_workers(worker_id)
  `)
    .eq("id", collectionId)
    .single();

  if (collectionError) {
    throw collectionError;
  }

  if (!collection) {
    throw new Error(`Collection '${collectionId}' not found.`);
  }

  const companyDocumentIds =
    collection.collection_company_documents.map(
      (x: { company_document_id: string }) => x.company_document_id
    );

  const workerIds =
    collection.collection_workers.map(
      (x: { worker_id: string }) => x.worker_id
    );

  const workerDocumentTypeIds =
    collection.collection_worker_document_types.map(
      (x: { worker_document_type_id: string }) =>
        x.worker_document_type_id
    );

  return {
    collectionId,
    companyDocumentIds,
    workerIds,
    workerDocumentTypeIds,
  };
}

async function generateZip(job: ZipJob) {
  const startedAt = Date.now();
  const collectionId = job.collection_id;

  console.log(`[${job.id}] Generating ZIP for collection ${collectionId}`);
  const relationships = await loadCollectionRelationships(collectionId);

  //
  // Load documents
  //
  const { data: companyDocs, error: companyError } =
    await supabase.schema("public").rpc("get_active_company_documents", {
      company_document_ids: relationships.companyDocumentIds,
    });

  if (companyError) throw companyError;

  const { data: workerDocs, error: workerError } =
    await supabase.schema("public").rpc("get_active_worker_documents", {
      worker_ids: relationships.workerIds,
      worker_document_type_ids: relationships.workerDocumentTypeIds,
    });

  if (workerError) throw workerError;

  const { data: uploadedDocs, error: uploadedError } = await supabase
    .from("collection_uploaded_documents")
    .select("file_name,file_path,file_size")
    .eq("document_collection_id", collectionId);

  if (uploadedError) throw uploadedError;

  const documents: ZipDocument[] = [];

  for (const doc of companyDocs ?? []) {
    documents.push({
      bucket: BUCKET,
      path: doc.file_path,
      zipPath: `company-documents/${sanitize(doc.document_type_name)}/${doc.file_name}`,
      size: doc.file_size ?? 0,
    });
  }

  for (const doc of workerDocs ?? []) {
    documents.push({
      bucket: BUCKET,
      path: doc.file_path,
      zipPath: `workers/${sanitize(doc.worker_name)}/${sanitize(
        doc.document_type_name
      )}/${doc.file_name}`,
      size: doc.file_size ?? 0,
    });
  }

  for (const doc of uploadedDocs ?? []) {
    documents.push({
      bucket: UPLOADED_BUCKET,
      path: doc.file_path,
      zipPath: `uploaded-documents/${doc.file_name}`,
      size: doc.file_size ?? 0,
    });
  }

  if (documents.length === 0) {
    throw new Error("Collection contains no documents.");
  }

  const totalBytes = documents.reduce((s, d) => s + d.size, 0);

  //
  // Preparing
  //
  await supabase
    .from("zip_jobs")
    .update({
      stage: "preparing",
      total_bytes: totalBytes,
      processed_bytes: 0,
      progress: 0,
      updated_at: new Date().toISOString(),
    })
    .eq("id", job.id);

  //
  // Create temp zip
  //
  const tempZip = path.join(TEMP_DIR, `${collectionId}.zip`);

  await fs.promises.rm(tempZip, {
    force: true,
  });

  const output = fs.createWriteStream(tempZip);

  const archive = archiver("zip", {
    zlib: {
      level: 9,
    },
  });

  archive.pipe(output);

  archive.on("warning", (err) => {
    console.warn(err);
  });

  archive.on("error", (err) => {
    throw err;
  });

  //
  // Heartbeat
  //
  const heartbeatTimer = setInterval(async () => {
    try {
      await heartbeat(job.id);
    } catch { }
  }, HEARTBEAT_INTERVAL);

  const throttledProgress = throttleAsync(
    (processed: number) =>
      updateProgress(job.id, processed, totalBytes),
    500
  );

  let processedBytes = 0;

  try {
    //
    // Download + append
    //
    await supabase
      .from("zip_jobs")
      .update({
        stage: "downloading",
      })
      .eq("id", job.id);

    for (const document of documents) {
      await appendDocument(
        archive,
        document,
        (bytes) => {
          processedBytes += bytes;
          throttledProgress(processedBytes);
        }
      );
    }

    //
    // Compress
    //
    await supabase
      .from("zip_jobs")
      .update({
        stage: "compressing",
      })
      .eq("id", job.id);

    await archive.finalize();

    await new Promise<void>((resolve, reject) => {
      output.on("close", resolve);
      output.on("error", reject);
    });

    //
    // Upload
    //
    await supabase
      .from("zip_jobs")
      .update({
        stage: "uploading",
        progress: 90,
      })
      .eq("id", job.id);

    const zipPath = `collections/${collectionId}.zip`;

    const zipStats = await fs.promises.stat(tempZip);

    await retry(async () => {
      const stream = fs.createReadStream(tempZip);

      const response = await fetch(
        `${SUPABASE_URL}/storage/v1/object/${BUCKET}/${zipPath}`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${SERVICE_ROLE_KEY}`,
            "Content-Type": "application/zip",
            "x-upsert": "true",
          },
          body: Readable.toWeb(stream),
          duplex: "half",
        } as RequestInit & { duplex: "half" | "full" }
      );

      if (!response.ok) {
        throw new Error(await response.text());
      }
    });

    //
    // Update collection
    //
    await supabase
      .from("document_collections")
      .update({
        zip_status: "ready",
        zip_path: zipPath,
        zip_generated_at: new Date().toISOString(),
        zip_size: zipStats.size,
      })
      .eq("id", collectionId);

    //
    // Complete job
    //
    await supabase
      .from("zip_jobs")
      .update({
        stage: "completed",
      })
      .eq("id", job.id);

    await completeJob(job.id);

    console.log(
      `[${job.id}] Finished in ${(
        (Date.now() - startedAt) /
        1000
      ).toFixed(1)}s`
    );
  } finally {
    clearInterval(heartbeatTimer);
    await throttledProgress.flush();
    await fs.promises.rm(tempZip, {
      force: true,
    });
  }
}

async function recoverStuckJobs() {
  await supabase.rpc("recover_stuck_zip_jobs");
}

function setupGracefulShutdown() {
  const shutdown = async (signal: string) => {
    console.log(`Received ${signal}, shutting down gracefully...`);
    shuttingDown = true;

    // If a job is running, mark it as stale-safe
    if (currentJobId) {
      await supabase
        .from("zip_jobs")
        .update({
          status: "running", // keep running; recovery will handle if needed
          updated_at: new Date().toISOString(),
        })
        .eq("id", currentJobId);
    }

    // Give streams time to flush
    setTimeout(() => {
      process.exit(0);
    }, 5000);
  };

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

async function main() {
  while (!shuttingDown) {
    const job = await claimJob();

    if (!job) {
      await sleep(2000);
      continue;
    }

    currentJobId = job.id;

    try {
      await generateZip(job);
    } catch (err) {
      console.error(err);
      await failJob(job.id, err);
    } finally {
      currentJobId = null;
    }
  }
}

await fs.promises.mkdir(TEMP_DIR, {
  recursive: true,
});

setupGracefulShutdown();
await recoverStuckJobs();
main();