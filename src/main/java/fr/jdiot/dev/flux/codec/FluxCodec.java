package fr.jdiot.dev.flux.codec;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;

/**
 * Interface for high-performance zero-copy serialization and deserialization.
 */
public interface FluxCodec<T> {
    
    /**
     * Serializes an object to a ByteBuf.
     */
    ByteBuf encode(T object);
    
    /**
     * Deserializes a ByteBuf to an object.
     */
    T decode(ByteBuf buffer);
    
    /**
     * Serializes a Flux of objects to a Flux of ByteBufs.
     */
    Flux<ByteBuf> encodeFlux(Flux<T> flux);
    
    /**
     * Deserializes a Flux of ByteBufs to a Flux of objects.
     */
    Flux<T> decodeFlux(Flux<ByteBuf> flux);
}
