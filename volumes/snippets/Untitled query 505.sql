SELECT * FROM zip_jobs zj
join collection_company_documents ccd
on zj.collection_id == ccd.collection_id
join collection_uploaded_documents cud
on zj.collection_id == cud.collection_id
join collection_worker_document_types cwdt
on zj.collection_id == cwdt.collection_id
join collection_workers cw
on zj.collection_id == cw.collection_id
join document_collections dc
on zj.collection_id == dc.collection_id
SELECT * FROM collection_company_documents;
SELECT * FROM collection_uploaded_documents;
SELECT * FROM collection_worker_document_types;
SELECT * FROM collection_workers;
SELECT * FROM document_collections;

-- 136e15e5-b116-44e1-8b10-49d52a426e0a