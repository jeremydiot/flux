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
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;

public class FramedFileCodec<M> implements FluxCodec<FluxFile<M>> {

  private final PojoCodec<M> metadataCodec;

  public FramedFileCodec(final PojoCodec<M> metadataCodec) {
    this.metadataCodec = metadataCodec;
  }

  @Override
  public Flux<ByteBuf> encode(final Flux<FluxFile<M>> flux) {
    return flux.concatMap(file -> {
      final ByteBuf metadataBuf = this.metadataCodec.encode(file.getMetadata());
      final int metadataLength = metadataBuf.readableBytes();
      final ByteBuf header1 = PooledByteBufAllocator.DEFAULT.buffer(4);
      header1.writeInt(metadataLength);

      final ByteBuf header2 = PooledByteBufAllocator.DEFAULT.buffer(4);
      // The protocol uses 4 bytes for data length (M)
      header2.writeInt((int) file.getDataLength());

      final Flux<ByteBuf> headers = Flux.just(header1, metadataBuf, header2);
      final Flux<ByteBuf> dataStream = file.getDataStream();

      return Flux.concat(headers, dataStream);
    });
  }

  @Override
  public Flux<FluxFile<M>> decode(final Flux<ByteBuf> flux) {
    return Flux.create(sink -> {
      final FramedDecoderSubscriber subscriber = new FramedDecoderSubscriber(sink);
      sink.onDispose(subscriber::cancel);
      flux.subscribe(subscriber);
    }, FluxSink.OverflowStrategy.BUFFER);
  }

  private enum Stage {
    READ_N_LENGTH, READ_METADATA, READ_M_LENGTH, READ_DATA
  }

  private class FramedDecoderSubscriber extends BaseSubscriber<ByteBuf> {
    private final FluxSink<FluxFile<M>> outerSink;
    private final CompositeByteBuf buffer = PooledByteBufAllocator.DEFAULT.compositeBuffer();

    private Stage stage = Stage.READ_N_LENGTH;
    private int metadataLength = 0;
    private M metadata = null;
    private long dataLength = 0;
    private long dataRead = 0;
    private Sinks.Many<ByteBuf> dataSink = null;
    private final AtomicLong innerDemand = new AtomicLong(0);

    public FramedDecoderSubscriber(final FluxSink<FluxFile<M>> outerSink) {
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
              final byte[] metadataBytes = new byte[this.metadataLength];
              metadataBuf.readBytes(metadataBytes);

              this.metadata = FramedFileCodec.this.metadataCodec.decode(metadataBytes);
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
              // Inner stream cancelled
            });

            final FluxFile<M> file = FluxFile.<M>builder().metadata(this.metadata).dataLength(this.dataLength)
                .dataStream(dataStream).build();

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
              final ByteBuf dataChunk = this.buffer.readRetainedSlice(toRead);
              this.dataSink.tryEmitNext(dataChunk);
              this.dataRead += toRead;
              this.innerDemand.decrementAndGet();
              this.buffer.discardReadComponents();
            }
          }
          if (this.dataRead == this.dataLength) {
            this.dataSink.tryEmitComplete();
            this.stage = Stage.READ_N_LENGTH;
            this.metadata = null;
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
