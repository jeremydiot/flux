package fr.jdiot.dev.flux.core;

import fr.jdiot.dev.flux.config.FluxProperties;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LazyFluxManagerImpl extends AbstractFluxManager {

  public LazyFluxManagerImpl(final FluxProperties properties) {
    super(properties);
  }

  /**
   * client push request or client push bridge request
   */
  @Override
  public Mono<Acknowledgement> registerFlux(final String fluxId, final Flux<ByteBuf> dataStream) {
    return this.internalRegisterFlux(fluxId, () -> dataStream);
  }

}
