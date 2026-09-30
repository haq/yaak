/* tslint:disable */
/* eslint-disable */

export function blob_delete(id: string): void;

export function blob_get(id: string): Uint8Array | undefined;

export function blob_put(id: string, bytes: Uint8Array): void;

/**
 * Register the IndexedDB-backed VFS and open the database.
 *
 * Migrations run inside `init_standalone`, exactly as they do for the CLI.
 * Safe to call more than once; later calls are no-ops.
 */
export function boot(): Promise<void>;

export function prepare_http_send(payload: any): Promise<any>;

export function rpc(cmd: string, payload: any, label: string): any;
