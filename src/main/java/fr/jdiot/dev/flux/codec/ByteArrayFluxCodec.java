package fr.jdiot.dev.flux.codec;

import fr.jdiot.dev.flux.exception.FluxException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;

/**
 * A FluxCodec implementation for raw byte arrays. Optimized for minimal memory
 * footprint and zero-copy encoding.
 */
public class ByteArrayFluxCodec implements FluxCodec<byte[]> {

  @Override
  public ByteBuf encode(final byte[] object) {
    try {
      // Zero-copy encode by wrapping the existing byte array.
      return Unpooled.wrappedBuffer(object);
    } catch (final Exception e) {
      throw new FluxException("Failed to encode byte array", e);
    }
  }

  @Override
  public byte[] decode(final ByteBuf buffer) {
    try {
      final int length = buffer.readableBytes();
      final byte[] bytes = new byte[length];
      buffer.readBytes(bytes);
      return bytes;
    } catch (final Exception e) {
      throw new FluxException("Failed to decode ByteBuf to byte array", e);
    }
  }

  @Override
  public Flux<ByteBuf> encodeFlux(final Flux<byte[]> flux) {
    return flux.map(this::encode);
  }

  @Override
  public Flux<byte[]> decodeFlux(final Flux<ByteBuf> flux) {
    return flux.map(this::decode);
  }
}
