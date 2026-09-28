import type { RpcTransport } from './';
import { diag, hex } from '../diag';

export async function connect(): Promise<RpcTransport> {
  let abortController = new AbortController();
  let port = await navigator.serial.requestPort({});

  await port.open({ baudRate: 12500 })
        .catch((e) => {
          if (e instanceof DOMException && e.name === "NetworkError") {
            throw new Error("Failed to open the serial port. Check the permissions of the device and verify it is not in use by another process.", { cause: e });
          } else {
            throw e;
          }
        });

  let info = port.getInfo();
  let label =
    (info.usbVendorId?.toLocaleString() || '') +
    ':' +
    (info.usbProductId?.toLocaleString() || '');

  diag(
    'serial port opened:',
    label,
    'usbVendorId=0x' + (info.usbVendorId?.toString(16) ?? '?'),
    'usbProductId=0x' + (info.usbProductId?.toString(16) ?? '?')
  );

  // DIAGNOSTIC: log every raw chunk written to the port.
  let txLog = new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      diag('RAW TX:', chunk.length, 'bytes:', hex(chunk));
      controller.enqueue(chunk);
    },
  });
  let txPipe = txLog.readable.pipeTo(port.writable!);
  txPipe.catch((e) => {
    diag('RAW TX: pipe to port failed:', String(e));
  });

  // DIAGNOSTIC: log every raw chunk arriving from the port.
  let rxLog = new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      diag('RAW RX:', chunk.length, 'bytes:', hex(chunk));
      controller.enqueue(chunk);
    },
  });
  let rxPipe = port.readable!.pipeTo(rxLog.writable);
  rxPipe.catch((e) => {
    diag('RAW RX: pipe from port failed:', String(e));
  });

  let sig = abortController.signal;
  let abort_cb: (this: AbortSignal, ev: Event) => any;

  abort_cb = async (ev: Event) => {
    sig.removeEventListener("abort", abort_cb);
    diag('serial port closing (abort)');
    try {
      await port.close();
    } catch (e) {
      diag('serial: port close error:', String(e));
    }
    diag('serial port closed');
  }

  sig.addEventListener("abort", abort_cb);

  return { label, abortController, readable: rxLog.readable, writable: txLog.writable };
}
