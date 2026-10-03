package fr.jdiot.dev.flux.codec;

import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Subscription;

import fr.jdiot.dev.flux.core.FluxFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

public class SequentialFluxCodec<M> implements FluxCodec<FluxFile<M>> {

  private final PojoCodec<M> metadataCodec;
  private final int maxConcurrency;
  private final int prefetch;

  public SequentialFluxCodec(final PojoCodec<M> metadataCodec) {
    this(metadataCodec, Queues.SMALL_BUFFER_SIZE, Queues.SMALL_BUFFER_SIZE);
  }

  public SequentialFluxCodec(final PojoCodec<M> metadataCodec, final int maxConcurrency, final int prefetch) {
    this.metadataCodec = metadataCodec;
    this.maxConcurrency = maxConcurrency;
    this.prefetch = prefetch;
  }

  @Override
  public Flux<ByteBuf> encode(final Flux<FluxFile<M>> flux) {
    return flux.flatMapSequential(file -> Flux.defer(() -> {
      if (file.getDataLength() < 0 || file.getDataLength() > 4294967295L) {
        return Flux.error(new IllegalArgumentException("File size exceeds protocol limit of 4GB"));
      }

      final ByteBuf metadataBuf = this.metadataCodec.encode(file.getMetadata());
      final int metadataLength = metadataBuf.readableBytes();

      final ByteBuf header = PooledByteBufAllocator.DEFAULT.buffer(8 + metadataLength);
      header.writeInt(metadataLength);
      header.writeBytes(metadataBuf);
      header.writeInt((int) file.getDataLength());
      metadataBuf.release();
      // Prepend header to the first data chunk to avoid separate onNext / HTTP frame
      final AtomicBoolean first = new AtomicBoolean(true);
      return file.getDataStream().map(chunk -> {
        if (first.compareAndSet(true, false)) {
          final CompositeByteBuf combined = PooledByteBufAllocator.DEFAULT.compositeBuffer(2);
          combined.addComponent(true, header);
          combined.addComponent(true, chunk);
          return (ByteBuf) combined;
        }
        return chunk;
      }).doOnError(e -> {
        if (first.get() && header.refCnt() > 0) {
          header.release();
        }
      }).doOnCancel(() -> {
        if (first.get() && header.refCnt() > 0) {
          header.release();
        }
      }).switchIfEmpty(Flux.defer(() -> Flux.just(header))); // If dataStream is empty, just emit header
    }), this.maxConcurrency, this.prefetch);
  }

  @Override
  public Flux<FluxFile<M>> decode(final Flux<ByteBuf> flux) {
    final Flux<RawFile> rawStream = Flux.create(sink -> {
      final FramedDecoderSubscriber subscriber = new FramedDecoderSubscriber(sink);
      sink.onDispose(subscriber::cancel);
      flux.subscribe(subscriber);
    }, FluxSink.OverflowStrategy.BUFFER);

    return rawStream.map(raw -> {
      final M metadata = this.metadataCodec.decode(raw.metadataBytes);
      return FluxFile.<M>builder().metadata(metadata).dataLength(raw.dataLength).dataStream(raw.dataStream).build();
    });
  }

  private static class RawFile {
    final byte[] metadataBytes;
    final long dataLength;
    final Flux<ByteBuf> dataStream;

    RawFile(final byte[] metadataBytes, final long dataLength, final Flux<ByteBuf> dataStream) {
      this.metadataBytes = metadataBytes;
      this.dataLength = dataLength;
      this.dataStream = dataStream;
    }
  }

  private enum Stage {
    READ_N_LENGTH, READ_METADATA, READ_M_LENGTH, READ_DATA, WAIT_FOR_DATA_FAST_PATH
  }

  /**
   * High-performance framed decoder subscriber.
   * <p>
   * Uses cumulation strategy: incoming chunks are accumulated into a single
   * contiguous buffer via Netty's cumulator pattern (same as
   * ByteToMessageDecoder). This avoids CompositeByteBuf O(n) component traversal
   * on readableBytes()/readInt() while keeping copy overhead minimal (only
   * residual unread bytes are copied at the start of each cumulation).
   * <p>
   * Upstream is requested unboundedly — backpressure is handled by H2C flow
   * control window at the network level.
   */
  private class FramedDecoderSubscriber extends BaseSubscriber<ByteBuf> {
    private final FluxSink<RawFile> outerSink;
    private ByteBuf cumulation;

    private Stage stage = Stage.READ_N_LENGTH;
    private int metadataLength = 0;
    private byte[] metadataBytes = null;
    private long dataLength = 0;
    private long dataRead = 0;
    private Sinks.Many<ByteBuf> dataSink = null;

    public FramedDecoderSubscriber(final FluxSink<RawFile> outerSink) {
      this.outerSink = outerSink;
    }

    @Override
    protected void hookOnSubscribe(final Subscription subscription) {
      this.request(Long.MAX_VALUE);
    }

    @Override
    protected void hookOnNext(final ByteBuf chunk) {
      this.cumulate(chunk);
      this.process();
    }

    /**
     * Cumulation strategy inspired by Netty's ByteToMessageDecoder.MERGE_CUMULATOR.
     * If the existing cumulation buffer has enough writable space, the new chunk is
     * appended directly. Otherwise, a new buffer is allocated with the combined
     * size. This avoids O(n) CompositeByteBuf component scans while keeping copy
     * overhead to a minimum — only residual unread bytes from the previous
     * cumulation are copied, not the full history.
     */
    private void cumulate(final ByteBuf chunk) {
      if (this.cumulation == null || !this.cumulation.isReadable()) {
        if (this.cumulation != null) {
          this.cumulation.release();
        }
        this.cumulation = chunk.retain();
      } else if (this.cumulation.writableBytes() >= chunk.readableBytes()) {
        this.cumulation.writeBytes(chunk);
      } else {
        final int required = this.cumulation.readableBytes() + chunk.readableBytes();
        int allocSize = required << 1;
        if (this.stage == Stage.WAIT_FOR_DATA_FAST_PATH && this.dataLength > allocSize) {
          allocSize = (int) this.dataLength + 1024; // Pre-allocate exactly what's needed plus some headroom
        }
        final ByteBuf newBuf = PooledByteBufAllocator.DEFAULT.buffer(allocSize);
        newBuf.writeBytes(this.cumulation);
        newBuf.writeBytes(chunk);
        this.cumulation.release();
        this.cumulation = newBuf;
      }
    }

    @Override
    protected void hookOnError(final Throwable throwable) {
      if (this.dataSink != null) {
        this.dataSink.tryEmitError(throwable);
      }
      this.outerSink.error(throwable);
      this.releaseCumulation();
    }

    @Override
    protected void hookOnComplete() {
      if ((this.cumulation != null && this.cumulation.readableBytes() > 0) || this.stage == Stage.READ_DATA) {
        final IllegalStateException err = new IllegalStateException("Incomplete frame at end of stream");
        if (this.dataSink != null) {
          this.dataSink.tryEmitError(err);
        }
        this.outerSink.error(err);
      } else {
        this.outerSink.complete();
      }
      this.releaseCumulation();
    }

    @Override
    protected void hookFinally(final SignalType type) {
      // Buffer is released in hookOnComplete/hookOnError
    }

    private void releaseCumulation() {
      if (this.cumulation != null && this.cumulation.refCnt() > 0) {
        this.cumulation.release();
        this.cumulation = null;
      }
    }

    private void process() {
      while (this.cumulation.readableBytes() > 0) {
        if (this.stage == Stage.READ_N_LENGTH) {
          if (this.cumulation.readableBytes() >= 4) {
            this.metadataLength = this.cumulation.readInt();
            this.stage = Stage.READ_METADATA;
          } else {
            break;
          }
        }

        if (this.stage == Stage.READ_METADATA) {
          if (this.cumulation.readableBytes() >= this.metadataLength) {
            this.metadataBytes = new byte[this.metadataLength];
            this.cumulation.readBytes(this.metadataBytes);
            this.stage = Stage.READ_M_LENGTH;
          } else {
            break;
          }
        }

        if (this.stage == Stage.READ_M_LENGTH) {
          if (this.cumulation.readableBytes() >= 4) {
            this.dataLength = this.cumulation.readUnsignedInt(); // 4 bytes unsigned
            this.dataRead = 0;

            if (this.dataLength == 0) {
              // Empty file
              final Flux<ByteBuf> dataStream = Flux.empty();
              final RawFile file = new RawFile(this.metadataBytes, this.dataLength, dataStream);
              this.outerSink.next(file);
              this.stage = Stage.READ_N_LENGTH;
              this.metadataBytes = null;
              this.metadataLength = 0;
              this.dataLength = 0;
              this.dataRead = 0;
            } else if (this.dataLength <= 4 * 1024 * 1024) { // 4MB
              // Small file -> wait for all data
              this.stage = Stage.WAIT_FOR_DATA_FAST_PATH;
            } else {
              // Large file -> stream
              this.stage = Stage.READ_DATA;
              this.dataSink = Sinks.many().unicast().onBackpressureBuffer();
              final Flux<ByteBuf> dataStream = this.dataSink.asFlux().doOnCancel(() -> {
              });
              final RawFile file = new RawFile(this.metadataBytes, this.dataLength, dataStream);
              this.outerSink.next(file);
            }
          } else {
            break;
          }
        }

        if (this.stage == Stage.WAIT_FOR_DATA_FAST_PATH) {
          if (this.cumulation.readableBytes() >= this.dataLength) {
            final ByteBuf completeData = this.cumulation.readRetainedSlice((int) this.dataLength);
            final Flux<ByteBuf> dataStream = Flux.just(completeData);
            final RawFile file = new RawFile(this.metadataBytes, this.dataLength, dataStream);
            this.outerSink.next(file);
            this.stage = Stage.READ_N_LENGTH;
            this.metadataBytes = null;
            this.metadataLength = 0;
            this.dataLength = 0;
            this.dataRead = 0;
          } else {
            break;
          }
        }

        if (this.stage == Stage.READ_DATA) {
          final long remainingData = this.dataLength - this.dataRead;
          if (remainingData > 0) {
            final int toRead = (int) Math.min(this.cumulation.readableBytes(), remainingData);
            if (toRead > 0) {
              final ByteBuf dataChunk = this.cumulation.readRetainedSlice(toRead);
              if (this.dataSink.tryEmitNext(dataChunk).isFailure()) {
                // If the sink is cancelled or overflows, we must release the chunk to prevent
                // memory leaks
                dataChunk.release();
              }
              this.dataRead += toRead;
            }
          }
          if (this.dataRead == this.dataLength) {
            this.dataSink.tryEmitComplete();
            this.stage = Stage.READ_N_LENGTH;
            this.metadataBytes = null;
            this.metadataLength = 0;
            this.dataLength = 0;
            this.dataRead = 0;
            this.dataSink = null;
          } else {
            break;
          }
        }
      }

      // Compact: release fully consumed cumulation to avoid holding references
      if (this.cumulation != null && !this.cumulation.isReadable()) {
        this.cumulation.release();
        this.cumulation = null;
      }
    }
  }
}
