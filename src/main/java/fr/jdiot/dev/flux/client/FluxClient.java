package fr.jdiot.dev.flux.client;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import io.netty.buffer.ByteBuf;
import fr.jdiot.dev.flux.core.Acknowledgement;

/**
 * Client for pulling from and pushing to the FLUX server.
 */
public interface FluxClient {
    
    /**
     * Pulls a data flux from the server.
     */
    Flux<ByteBuf> pull(String fluxId);
    
    /**
     * Pushes a data flux to the server and returns the acknowledgement.
     */
    Mono<Acknowledgement> push(String fluxId, Flux<ByteBuf> dataStream);
}
