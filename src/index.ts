import { Request, Response, RequestResponse, Notification } from './studio';

import { get_encoder, get_decoder } from './framing';
import { RpcTransport } from './transport';
import { diag, ts } from './diag';

import { Mutex } from 'async-mutex';
import { ErrorConditions } from './meta';
export { Request, RequestResponse, Response, Notification };

export interface RpcConnection {
  label: string;
  request_response_readable: ReadableStream<RequestResponse>;
  request_writable: WritableStream<Request>;
  notification_readable: ReadableStream<Notification>;
  current_request: number;
}

export interface CreateRpcConnectionOpts {
  signal?: AbortSignal;
}

function request_name(req: Request): string {
  const fields = [
    'core',
    'behaviors',
    'keymap',
    'sensors',
    'macros',
    'touchpad',
  ] as const;
  for (const f of fields) {
    if (req[f]) return f;
  }
  return 'unknown';
}

function response_name(rr: RequestResponse): string {
  const fields = [
    'meta',
    'core',
    'behaviors',
    'keymap',
    'sensors',
    'macros',
    'touchpad',
  ] as const;
  for (const f of fields) {
    if (rr[f]) return f;
  }
  return 'unknown';
}

function notification_name(n: Notification): string {
  const fields = ['core', 'keymap', 'sensors', 'macros', 'touchpad'] as const;
  for (const f of fields) {
    if (n[f]) return f;
  }
  return 'unknown';
}

export function create_rpc_connection(transport: RpcTransport, opts?: CreateRpcConnectionOpts): RpcConnection {
  diag('create_rpc_connection for transport:', transport.label);

  let { writable: request_writable, readable: byte_readable } =
    new TransformStream<Request, Uint8Array>({
      transform(chunk, controller) {
        let bytes = Request.encode(chunk).finish();
        diag(
          'REQUEST #' + chunk.requestId + ' (' + request_name(chunk) + ') encoded to',
          bytes.length,
          'bytes'
        );
        controller.enqueue(bytes);
      },
    });

  let reqPipelineClosed = byte_readable
    .pipeThrough(new TransformStream(get_encoder()), { signal: opts?.signal })
    .pipeTo(transport.writable, { signal: opts?.signal });

  reqPipelineClosed.catch((r) => { diag('request pipeline closed with error:', String(r)); return r }).then(async (reason: any) => {
    diag('request pipeline closed, reason:', String(reason));
    await byte_readable.cancel();
    transport.abortController.abort(reason);
  });

  let response_readable = transport.readable
    .pipeThrough(new TransformStream(get_decoder()), { signal: opts?.signal })
    .pipeThrough(
      new TransformStream({
        transform(chunk, controller) {
          let decoded = Response.decode(chunk);
          if (decoded.requestResponse) {
            diag(
              'DECODED response frame: requestId',
              decoded.requestResponse.requestId,
              'field',
              response_name(decoded.requestResponse),
              'from',
              chunk.length,
              'payload bytes'
            );
          } else if (decoded.notification) {
            diag(
              'DECODED notification frame: field',
              notification_name(decoded.notification),
              'from',
              chunk.length,
              'payload bytes'
            );
          } else {
            diag('DECODED empty frame from', chunk.length, 'payload bytes');
          }
          controller.enqueue(decoded);
        },
      }),
      { signal: opts?.signal }
    );

  let [a, b] = response_readable.tee();

  let request_response_readable = a.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        if (chunk.requestResponse) {
          diag(
            'RESPONSE branch: consumed response requestId',
            chunk.requestResponse.requestId,
            'field',
            response_name(chunk.requestResponse)
          );
          controller.enqueue(chunk.requestResponse);
        }
      },
    }),
    { signal: opts?.signal }
  );

  let notification_readable = b.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        if (chunk.notification) {
          diag(
            'NOTIFICATION branch: consumed notification field',
            notification_name(chunk.notification)
          );
          controller.enqueue(chunk.notification);
        }
      },
    }),
    { signal: opts?.signal }
  );

  return {
    label: transport.label,
    request_response_readable,
    request_writable,
    notification_readable,
    current_request: 0,
  };
}

const rpcMutex = new Mutex();

export class NoResponseError extends Error {
  constructor() {
    super("No RPC response received");
    Object.setPrototypeOf(this, NoResponseError.prototype);
  }
}

export class MetaError extends Error {
  readonly condition: ErrorConditions;

  constructor(condition: ErrorConditions) {
    super("Meta error: " + condition);
    this.condition = condition;
    Object.setPrototypeOf(this, MetaError.prototype);
  }
}

/**
 * DIAGNOSTIC: maximum time to wait for an RPC response before giving up.
 * Without this, a stalled read holds the RPC mutex forever and no further
 * calls can be made. Remove for production.
 */
const RPC_READ_TIMEOUT_MS = 10000;

export class RpcReadTimeoutError extends Error {
  readonly requestId: number;

  constructor(requestId: number) {
    super(
      'No RPC response for request ' +
        requestId +
        ' within ' +
        RPC_READ_TIMEOUT_MS +
        'ms'
    );
    this.requestId = requestId;
    Object.setPrototypeOf(this, RpcReadTimeoutError.prototype);
  }
}

export async function call_rpc(
  conn: RpcConnection,
  req: Omit<Request, 'requestId'>
): Promise<RequestResponse> {
  return await rpcMutex.runExclusive(async () => {
    let request: Request = { ...req, requestId: conn.current_request++ };

    diag(
      'call_rpc: request #' + request.requestId + ' (' + request_name(request) + ') — writing'
    );

    let writer = conn.request_writable.getWriter();
    await writer.write(request);
    writer.releaseLock();
    diag('call_rpc: request #' + request.requestId + ' written at', ts());

    let reader = conn.request_response_readable.getReader();

    let readPromise = reader.read();
    // If the timeout below fires first, releaseLock() rejects this pending
    // read with a TypeError (per the Streams spec); handle it here so it is
    // not reported as an unhandled rejection.
    readPromise.catch((e) => {
      diag('call_rpc: pending read rejected after release:', String(e));
    });

    let timer: ReturnType<typeof setTimeout> | undefined;
    let timeoutPromise = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        diag(
          'call_rpc: TIMEOUT — no response for request #' +
            request.requestId +
            ' after',
          RPC_READ_TIMEOUT_MS,
          'ms'
        );
        reject(new RpcReadTimeoutError(request.requestId));
      }, RPC_READ_TIMEOUT_MS);
    });

    try {
      let { done, value } = await Promise.race([readPromise, timeoutPromise]);

      if (done || !value) {
        diag(
          'call_rpc: stream closed, no response for request #' +
            request.requestId
        );
        throw 'No response';
      }

      if (value.requestId != request.requestId) {
        diag(
          'call_rpc: MISMATCH — got response #' +
            value.requestId +
            ' for request #' +
            request.requestId
        );
        throw 'Mismatch request IDs';
      }

      diag('call_rpc: response #' + value.requestId + ' received at', ts());

      if (value.meta?.noResponse) {
        throw new NoResponseError();
      } else if (value.meta?.simpleError) {
        throw new MetaError(value.meta.simpleError);
      }

      return value;
    } finally {
      if (timer) {
        clearTimeout(timer);
      }
      reader.releaseLock();
    }
  });
}
