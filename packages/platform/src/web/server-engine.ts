/** Database transport used when the self-hosted server owns SQLite. */

import { serverBaseUrl } from "./server";

export interface DatabaseEngine {
  rpc(this: void, cmd: string, payload: unknown, label: string): unknown;
  prepare_http_send(this: void, payload: unknown): Promise<unknown>;
  blob_get(this: void, id: string): Uint8Array | undefined | null | Promise<Uint8Array | null>;
  blob_put(this: void, id: string, bytes: Uint8Array): void | Promise<void>;
  blob_delete(this: void, id: string): void | Promise<void>;
}

/**
 * Fail closed when discovery fails. Falling back to IndexedDB would let the
 * user edit a different database while believing they were saving shared data.
 * An older Yaak server has no storage field and uses the original browser mode.
 */
export async function storageMode(): Promise<"server" | "browser"> {
  const response = await fetch(`${serverBaseUrl()}/v1/health`, {
    cache: "no-store",
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) throw new Error("Couldn't determine Yaak's storage mode. Reload to try again.");
  const health = (await response.json()) as { storage?: string; ok?: boolean };
  if (health.ok !== true) throw new Error("The Yaak server returned an invalid health response.");
  if (health.storage === "server") return "server";
  if (health.storage == null || health.storage === "browser") return "browser";
  throw new Error(`Unsupported Yaak storage mode: ${health.storage}`);
}

async function checked(response: Response): Promise<Response> {
  if (response.ok) return response;
  const body = await response.text();
  let reason = `Shared storage returned HTTP ${response.status}`;
  try {
    const parsed = JSON.parse(body) as { error?: string };
    if (parsed.error) reason = parsed.error;
  } catch {
    // Proxy failures can return HTML; keep the error useful without displaying it.
  }
  throw new Error(reason);
}

export function createServerEngine(): DatabaseEngine {
  const base = `${serverBaseUrl()}/v1/storage`;
  // Preserve the worker's original command ordering across network round trips.
  // A failed command doesn't prevent later commands, and writes aren't retried.
  let queue: Promise<void> = Promise.resolve();
  function ordered<T>(run: () => Promise<T>): Promise<T> {
    const result = queue.then(run);
    queue = result.then(
      () => {},
      () => {},
    );
    return result;
  }
  async function json(path: string, body: unknown): Promise<unknown> {
    const response = await checked(
      await fetch(`${base}/${path}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      }),
    );
    return response.json();
  }
  const blobUrl = (id: string) => `${base}/blobs/${encodeURIComponent(id)}`;

  return {
    rpc: (cmd, payload, label) => ordered(() => json("rpc", { cmd, payload, label })),
    prepare_http_send: (payload) => ordered(() => json("prepare", payload)),
    blob_get: (id) =>
      ordered(async () => {
        const response = await fetch(blobUrl(id), { cache: "no-store" });
        if (response.status === 404) return null;
        await checked(response);
        return new Uint8Array(await response.arrayBuffer());
      }),
    blob_put: (id, bytes) => {
      // Own the bytes while this upload waits behind earlier commands.
      const copy = new Uint8Array(bytes);
      return ordered(async () => {
        await checked(
          await fetch(blobUrl(id), {
            method: "POST",
            headers: { "content-type": "application/octet-stream" },
            body: copy,
          }),
        );
      });
    },
    blob_delete: (id) =>
      ordered(async () => {
        await checked(await fetch(blobUrl(id), { method: "DELETE" }));
      }),
  };
}
