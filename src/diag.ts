// Diagnostic instrumentation for the USB CDC-ACM RPC timeout investigation.
//
// Every line is prefixed with "[zmk-rpc]" so it can be filtered in the
// browser console. Timestamps are relative (ms) to the first log line so
// they can be correlated with device-side log timestamps.
//
// This file and the diag() calls scattered through the library are
// temporary; remove them once the issue is resolved.

function clock(): number {
  // performance.now() gives sub-millisecond resolution; fall back for
  // non-browser environments (tests).
  return typeof performance !== 'undefined' ? performance.now() : Date.now();
}

const t0 = clock();

export function ts(): string {
  return 't+' + (clock() - t0).toFixed(1) + 'ms';
}

export function hex(bytes: Uint8Array, max = 512): string {
  const n = Math.min(bytes.length, max);
  const parts: string[] = [];
  for (let i = 0; i < n; i++) {
    parts.push(bytes[i].toString(16).padStart(2, '0'));
  }
  let s = parts.join(' ');
  if (bytes.length > max) {
    s += ' ... (+' + (bytes.length - max) + ' more)';
  }
  return s;
}

export function diag(...args: unknown[]): void {
  const formatted: string[] = ['[zmk-rpc]', ts()];
  for (const a of args) {
    formatted.push(a instanceof Uint8Array ? hex(a) : String(a));
  }
  console.log(formatted.join(' '));
}
