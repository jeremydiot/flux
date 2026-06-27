package fr.jdiot.dev.flux.server;

import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

/**
 * Server interface for handling incoming requests and routing data.
 */
public interface FluxServer {

  /**
   * Starts the FLUX HTTP server.
   */
  Mono<? extends DisposableServer> start();

  /**
   * Stops the FLUX HTTP server.
   */
  void stop();
}
