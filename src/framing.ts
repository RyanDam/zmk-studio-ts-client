import { diag, hex } from './diag';

const FRAMING_SOF = 0xab;
const FRAMING_ESC = 0xac;
const FRAMING_EOF = 0xad;

export function get_encoder(): Transformer<Uint8Array, Uint8Array> {
  let frameCount = 0;
  return {
    transform: (chunk, controller) => {
      if (chunk instanceof Uint8Array) {
        frameCount++;
        diag(
          'ENCODER frame #' + frameCount + ' in:',
          chunk.length,
          'bytes:',
          hex(chunk)
        );
        controller.enqueue(new Uint8Array([FRAMING_SOF]));
        let next_start_index = 0;
        for (let i = 0; i < chunk.length; i++) {
          switch (chunk[i]) {
            case FRAMING_SOF:
            case FRAMING_ESC:
            case FRAMING_EOF:
              controller.enqueue(chunk.subarray(next_start_index, i));
              controller.enqueue(new Uint8Array([FRAMING_ESC]));
              next_start_index = i;
          }
        }

        if (next_start_index < chunk.length) {
          controller.enqueue(chunk.subarray(next_start_index, chunk.length));
        }
        controller.enqueue(new Uint8Array([FRAMING_EOF]));
      } else {
        return controller.error(
          'Only Uint8Array chunks are able to be handled'
        );
      }
    },
  };
}

enum DecodeState {
  IDLE = 0,
  AWAITING_DATA = 1,
  ESCAPED = 2,
}

export function get_decoder(): Transformer<Uint8Array, Uint8Array> {
  let state = DecodeState.IDLE;
  let data: Array<number> = [];
  let frameCount = 0;
  let byteCount = 0;

  let process = (
    b: number,
    controller: TransformStreamDefaultController<Uint8Array>
  ) => {
    switch (state) {
      case DecodeState.IDLE:
        switch (b) {
          case FRAMING_SOF:
            state = DecodeState.AWAITING_DATA;
            break;
          default:
            diag(
              'DECODER ERROR: byte 0x' +
                b.toString(16) +
                ' in IDLE (expected SoF 0xab), byte #' +
                byteCount
            );
            return controller.error('Expected SoF to start decoding');
        }
        break;
      case DecodeState.AWAITING_DATA:
        switch (b) {
          case FRAMING_SOF:
            diag(
              'DECODER ERROR: unexpected SoF mid-frame, byte #' +
                byteCount +
                ', partial frame (' +
                data.length +
                ' bytes):',
              hex(new Uint8Array(data))
            );
            return controller.error('Unexpected SoF mid-frame');
          case FRAMING_ESC:
            state = DecodeState.ESCAPED;
            break;
          case FRAMING_EOF:
            frameCount++;
            diag(
              'DECODER frame #' +
                frameCount +
                ' complete,',
              data.length,
              'payload bytes:',
              hex(new Uint8Array(data))
            );
            controller.enqueue(new Uint8Array(data));
            data = [];
            state = DecodeState.IDLE;
            break;
          default:
            data.push(b);
            break;
        }
        break;
      case DecodeState.ESCAPED:
        data.push(b);
        state = DecodeState.AWAITING_DATA;
        break;
    }
    byteCount++;
    return true;
  };

  return {
    transform(chunk, controller) {
      if (chunk instanceof Uint8Array) {
        diag('DECODER chunk in:', chunk.length, 'bytes:', hex(chunk));
        for (let i = 0; i < chunk.length; i++) {
          let b = chunk[i];
          if (!process(b, controller)) {
            throw 'Failed to process the byte';
          }
        }
      } else if (typeof chunk == 'number') {
        process(chunk, controller);
      } else {
        return controller.error(
          'Only Uint8Array chunks are able to be handled'
        );
      }
    },
    flush() {
      if (state !== DecodeState.IDLE || data.length > 0) {
        diag(
          'DECODER flush with incomplete frame: state=' +
            state +
            ', partial (' +
            data.length +
            ' bytes):',
          hex(new Uint8Array(data))
        );
      }
    },
  };
}
