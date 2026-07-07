package fr.jdiot.dev.flux.codec;

import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscription;

import fr.jdiot.dev.flux.core.FluxFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
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
    return flux.flatMapSequential(file -> Mono.fromCallable(() -> {
      final ByteBuf metadataBuf = this.metadataCodec.encode(file.getMetadata());
      final int metadataLength = metadataBuf.readableBytes();
      final ByteBuf header1 = PooledByteBufAllocator.DEFAULT.buffer(4);
      header1.writeInt(metadataLength);

      final ByteBuf header2 = PooledByteBufAllocator.DEFAULT.buffer(4);

      // The protocol uses 4 bytes for data length (M)
      // Validate file size, for prevent signed integer overflow. limit is ~4.29 Go
      if (file.getDataLength() < 0 || file.getDataLength() > 4294967295L) {
        throw new IllegalArgumentException("File size exceeds protocol limit of 4GB");
      }
      header2.writeInt((int) file.getDataLength());

      final Flux<ByteBuf> headers = Flux.just(header1, metadataBuf, header2);
      final Flux<ByteBuf> dataStream = file.getDataStream();

      return Flux.concat(headers, dataStream);
    }).subscribeOn(Schedulers.boundedElastic()).flatMapMany(f -> f), this.maxConcurrency, this.prefetch);
  }

  @Override
  public Flux<FluxFile<M>> decode(final Flux<ByteBuf> flux) {
    final Flux<RawFile> rawStream = Flux.create(sink -> {
      final FramedDecoderSubscriber subscriber = new FramedDecoderSubscriber(sink);
      sink.onDispose(subscriber::cancel);
      flux.subscribe(subscriber);
    }, FluxSink.OverflowStrategy.BUFFER);

    return rawStream.flatMapSequential(raw -> Mono.fromCallable(() -> {
      final M metadata = this.metadataCodec.decode(raw.metadataBytes);
      return FluxFile.<M>builder().metadata(metadata).dataLength(raw.dataLength).dataStream(raw.dataStream).build();
    }).subscribeOn(Schedulers.boundedElastic()), this.maxConcurrency, this.prefetch);
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
    READ_N_LENGTH, READ_METADATA, READ_M_LENGTH, READ_DATA
  }

  private class FramedDecoderSubscriber extends BaseSubscriber<ByteBuf> {
    private final FluxSink<RawFile> outerSink;
    private final CompositeByteBuf buffer = PooledByteBufAllocator.DEFAULT.compositeBuffer();

    private Stage stage = Stage.READ_N_LENGTH;
    private int metadataLength = 0;
    private byte[] metadataBytes = null;
    private long dataLength = 0;
    private long dataRead = 0;
    private Sinks.Many<ByteBuf> dataSink = null;
    private final AtomicLong innerDemand = new AtomicLong(0);

    public FramedDecoderSubscriber(final FluxSink<RawFile> outerSink) {
      this.outerSink = outerSink;
    }

    @Override
    protected void hookOnSubscribe(final Subscription subscription) {
      this.request(1);
    }

    @Override
    protected void hookOnNext(final ByteBuf chunk) {
      this.buffer.addComponent(true, chunk.retain());
      this.process();
    }

    @Override
    protected void hookOnError(final Throwable throwable) {
      if (this.dataSink != null) {
        this.dataSink.tryEmitError(throwable);
      }
      this.outerSink.error(throwable);
      this.buffer.release();
    }

    @Override
    protected void hookOnComplete() {
      if (this.buffer.readableBytes() > 0 || this.stage == Stage.READ_DATA) {
        final IllegalStateException err = new IllegalStateException("Incomplete frame at end of stream");
        if (this.dataSink != null) {
          this.dataSink.tryEmitError(err);
        }
        this.outerSink.error(err);
      } else {
        this.outerSink.complete();
      }
      this.buffer.release();
    }

    @Override
    protected void hookFinally(final SignalType type) {
      // Buffer is released in hookOnComplete/hookOnError
    }

    private void process() {
      while (this.buffer.readableBytes() > 0) {
        if (this.stage == Stage.READ_N_LENGTH) {
          if (this.buffer.readableBytes() >= 4) {
            this.metadataLength = this.buffer.readInt();
            this.stage = Stage.READ_METADATA;
            this.buffer.discardReadComponents();
          } else {
            break;
          }
        }

        if (this.stage == Stage.READ_METADATA) {
          if (this.buffer.readableBytes() >= this.metadataLength) {
            final ByteBuf metadataBuf = this.buffer.readRetainedSlice(this.metadataLength);

            try {
              this.metadataBytes = new byte[this.metadataLength];
              metadataBuf.readBytes(this.metadataBytes);
            } finally {
              metadataBuf.release();
            }
            this.stage = Stage.READ_M_LENGTH;
            this.buffer.discardReadComponents();
          } else {
            break;
          }
        }

        if (this.stage == Stage.READ_M_LENGTH) {
          if (this.buffer.readableBytes() >= 4) {
            this.dataLength = this.buffer.readUnsignedInt(); // 4 bytes unsigned
            this.stage = Stage.READ_DATA;
            this.dataRead = 0;
            this.innerDemand.set(0);

            this.dataSink = Sinks.many().unicast().onBackpressureBuffer();

            final Flux<ByteBuf> dataStream = this.dataSink.asFlux().doOnRequest(n -> {
              this.innerDemand.addAndGet(n);
              this.request(1);
            }).doOnCancel(() -> {
              // If the consumer cancels the file download midway, we MUST drain the rest 
              // of the bytes from the network socket to avoid corrupting the stream for the next file.
              this.innerDemand.set(Long.MAX_VALUE); // Fake infinite demand to drain
              this.request(1); // Wake up Netty
            });

            final RawFile file = new RawFile(this.metadataBytes, this.dataLength, dataStream);

            this.outerSink.next(file);
            this.buffer.discardReadComponents();
          } else {
            break;
          }
        }

        if (this.stage == Stage.READ_DATA) {
          final long remainingData = this.dataLength - this.dataRead;
          if (remainingData > 0) {
            final int toRead = (int) Math.min(this.buffer.readableBytes(), remainingData);
            if (toRead > 0) {
              final ByteBuf dataChunk = this.buffer.alloc().buffer(toRead);
              this.buffer.readBytes(dataChunk);
              if (this.dataSink.tryEmitNext(dataChunk).isFailure()) {
                // If the sink is cancelled or overflows, we must release the chunk to prevent memory leaks
                dataChunk.release();
              }
              this.dataRead += toRead;
              this.innerDemand.decrementAndGet();
              this.buffer.discardReadComponents();
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

      if (this.stage != Stage.READ_DATA) {
        this.request(1);
      } else if (this.innerDemand.get() > 0 && this.buffer.readableBytes() == 0) {
        this.request(1);
      }
    }
  }
}
