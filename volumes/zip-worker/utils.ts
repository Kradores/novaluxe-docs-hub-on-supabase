const MAX_UPLOAD_RETRIES = 3;

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

const sanitize = (v: string) =>
  v.replace(/[\/\\?%*:|"<>]/g, "-").slice(0, 100);

async function retry<T>(
  operation: () => Promise<T>,
  retries = MAX_UPLOAD_RETRIES
): Promise<T> {
  let delay = 2000;

  let lastError: unknown;

  for (let i = 0; i < retries; i++) {
    try {
      return await operation();
    } catch (err) {
      lastError = err;

      if (i === retries - 1)
        break;

      console.warn(
        `Retry ${i + 1}/${retries} after ${delay}ms`
      );

      await sleep(delay);

      delay *= 2;
    }
  }

  throw lastError;
}

function throttleAsync<T extends unknown[]>(
  fn: (...args: T) => Promise<void>,
  delay: number
) {
  let lastCall = 0;
  let timeout: NodeJS.Timeout | null = null;
  let lastArgs: T | null = null;

  const throttled = (...args: T) => {
    lastArgs = args;

    const now = Date.now();

    const invoke = () => {
      lastCall = Date.now();
      timeout = null;

      if (lastArgs) {
        void fn(...lastArgs);
      }
    };

    if (now - lastCall >= delay) {
      invoke();
      return;
    }

    if (!timeout) {
      timeout = setTimeout(
        invoke,
        delay - (now - lastCall)
      );
    }
  };

  throttled.flush = async () => {
    if (timeout) {
      clearTimeout(timeout);
      timeout = null;
    }

    if (lastArgs) {
      await fn(...lastArgs);
    }
  };

  return throttled;
}

export {
  sanitize,
  retry,
  throttleAsync,
  sleep,
};