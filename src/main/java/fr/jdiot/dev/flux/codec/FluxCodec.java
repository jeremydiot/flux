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
   * Deserializes a byte array to an object.
   */
  default T decode(byte[] bytes) {
    return decode(io.netty.buffer.Unpooled.wrappedBuffer(bytes));
  }

  /**
   * Serializes a Flux of objects to a Flux of ByteBufs.
   */
  default Flux<ByteBuf> encodeFlux(final Flux<T> flux) {
    return flux.map(this::encode);
  }

  /**
   * Deserializes a Flux of ByteBufs to a Flux of objects.
   */
  default Flux<T> decodeFlux(final Flux<ByteBuf> flux) {
    return flux.map(this::decode);
  }
}
