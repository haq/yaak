import { afterEach, describe, expect, it, vi } from "vitest";
import { createServerEngine, storageMode } from "./server-engine";

afterEach(() => vi.unstubAllGlobals());

describe("storage discovery", () => {
  it("supports shared storage and the original stateless server", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(Response.json({ ok: true, storage: "server" }))
      .mockResolvedValueOnce(Response.json({ ok: true, storage: "browser" }))
      .mockResolvedValueOnce(Response.json({ ok: true }));
    vi.stubGlobal("fetch", fetch);
    expect(await storageMode()).toBe("server");
    expect(await storageMode()).toBe("browser");
    expect(await storageMode()).toBe("browser");
  });

  it("never switches to a local database when shared storage is unreachable", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("Network unavailable")));
    await expect(storageMode()).rejects.toThrow("Network unavailable");
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(Response.json({ error: "Unavailable" }, { status: 503 })),
    );
    await expect(storageMode()).rejects.toThrow("storage mode");
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(Response.json({ ok: true, storage: "unknown" })),
    );
    await expect(storageMode()).rejects.toThrow("Unsupported");
  });
});

describe("server database transport", () => {
  it("preserves write order and continues after a failed command without retrying it", async () => {
    let finishFirst!: (response: Response) => void;
    const fetch = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise<Response>((resolve) => {
            finishFirst = resolve;
          }),
      )
      .mockResolvedValueOnce(Response.json({ result: "second", events: [] }));
    vi.stubGlobal("fetch", fetch);
    const engine = createServerEngine();
    const first = engine.rpc("models_upsert", { model: { name: "first" } }, "tab_a");
    const rejection = expect(first).rejects.toThrow("Write failed");
    const second = engine.rpc("models_upsert", { model: { name: "second" } }, "tab_a");
    await Promise.resolve();
    expect(fetch).toHaveBeenCalledTimes(1);
    finishFirst(Response.json({ error: "Write failed" }, { status: 400 }));
    await rejection;
    expect(await second).toEqual({ result: "second", events: [] });
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(JSON.parse(fetch.mock.calls[1]![1].body)).toEqual({
      cmd: "models_upsert",
      payload: { model: { name: "second" } },
      label: "tab_a",
    });
  });

  it("uploads binary bodies intact and distinguishes missing bodies from server failures", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response(new Uint8Array([0, 128, 255])))
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
      .mockResolvedValueOnce(Response.json({ error: "Database unavailable" }, { status: 500 }));
    vi.stubGlobal("fetch", fetch);
    const engine = createServerEngine();
    const bytes = new Uint8Array([0, 128, 255]);
    const pending = engine.blob_put("rs_body.request", bytes);
    bytes.fill(1);
    await pending;
    expect(fetch.mock.calls[0]![1].body).toEqual(new Uint8Array([0, 128, 255]));
    expect(await engine.blob_get("rs_body.request")).toEqual(new Uint8Array([0, 128, 255]));
    expect(await engine.blob_get("missing")).toBeNull();
    await expect(engine.blob_get("rs_body.request")).rejects.toThrow("Database unavailable");
  });
});
