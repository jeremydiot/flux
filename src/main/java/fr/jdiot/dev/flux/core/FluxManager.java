package fr.jdiot.dev.flux.core;

import java.util.Set;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;

/**
 * Core logic for flux pooling, back pressure management, and data routing.
 */
public interface FluxManager {

  /**
   * Registers a new incoming flux and returns a Mono that emits the final
   * acknowledgement.
   */
  reactor.core.publisher.Mono<Acknowledgement> registerFlux(String fluxId, Flux<ByteBuf> dataStream);

  /**
   * Retrieves an existing flux.
   */
  Flux<ByteBuf> getFlux(String fluxId);

  /**
   * Acknowledges the successful transfer of a flux.
   */
  void acknowledge(String fluxId, Acknowledgement ack);

  /**
   * Returns the set of active flux IDs.
   */
  Set<String> getActiveFluxIds();
}
