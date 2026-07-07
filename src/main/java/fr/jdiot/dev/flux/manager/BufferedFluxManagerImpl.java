package fr.jdiot.dev.flux.manager;

import fr.jdiot.dev.flux.core.Acknowledgement;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public class BufferedFluxManagerImpl extends AbstractFluxManager {

  public BufferedFluxManagerImpl(final FluxManagerProperties properties) {
    super(properties);
  }

  /**
   * client push request or client push bridge request
   */
  @Override
  public Mono<Acknowledgement> registerFlux(final String fluxId, final Flux<ByteBuf> dataStream) {

    return this.internalRegisterFlux(fluxId, () -> {
      final int bufferSize = this.properties.getBackPressureSize();
      final Sinks.Many<ByteBuf> dataSink = Sinks.many().unicast().onBackpressureBuffer();

      final reactor.core.publisher.BaseSubscriber<ByteBuf> subscriber = new reactor.core.publisher.BaseSubscriber<ByteBuf>() {
        @Override
        protected void hookOnSubscribe(final org.reactivestreams.Subscription subscription) {
          this.request(bufferSize);
        }

        @Override
        protected void hookOnNext(final ByteBuf value) {
          if (dataSink.tryEmitNext(value).isFailure()) {
            ReferenceCountUtil.safeRelease(value);
          }
        }

        @Override
        protected void hookOnError(final Throwable throwable) {
          dataSink.tryEmitError(throwable);
        }

        @Override
        protected void hookOnComplete() {
          dataSink.tryEmitComplete();
        }
      };

      dataStream.subscribe(subscriber);

      return dataSink.asFlux().doOnRequest(n -> subscriber.request(n)).doFinally(_ -> subscriber.cancel());
    });

  }

}
