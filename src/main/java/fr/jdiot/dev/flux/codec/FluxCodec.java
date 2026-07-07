package fr.jdiot.dev.flux.codec;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;

/**
 * Interface for high-performance zero-copy serialization and deserialization.
 */
public interface FluxCodec<T> {

  /**
   * Serializes a Flux of objects to a Flux of ByteBufs.
   */
  Flux<ByteBuf> encode(Flux<T> flux);

  /**
   * Deserializes a Flux of ByteBufs to a Flux of objects.
   */
  Flux<T> decode(Flux<ByteBuf> flux);
}
