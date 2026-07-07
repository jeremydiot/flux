package fr.jdiot.dev.flux.manager;

import fr.jdiot.dev.flux.core.Acknowledgement;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

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

      final Sinks.Many<ByteBuf> dataSink = Sinks.many().unicast()
          .onBackpressureBuffer(Queues.<ByteBuf>get(this.properties.getBackPressureSize()).get());

      final Disposable subscription = dataStream.subscribe(buf -> {
        final Sinks.EmitResult result = dataSink.tryEmitNext(buf);

        if (result.isFailure()) {
          ReferenceCountUtil.safeRelease(buf);
          dataSink.tryEmitError(new IllegalStateException("Backpressure overflow"));
        }
      }, err -> {
        dataSink.tryEmitError(err != null ? err : new IllegalStateException("Unknown flux error"));
      }, () -> dataSink.tryEmitComplete());

      return dataSink.asFlux().doFinally(_ -> subscription.dispose());
    });

  }

}
