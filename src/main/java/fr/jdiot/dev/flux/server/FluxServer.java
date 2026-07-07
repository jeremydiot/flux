package fr.jdiot.dev.flux.server;

import reactor.netty.DisposableServer;

/**
 * Server interface for handling incoming requests and routing data.
 */
public interface FluxServer {

  /**
   * Starts the FLUX HTTP server.
   */
  DisposableServer start();

  /**
   * Stops the FLUX HTTP server.
   */
  void stop();
}
