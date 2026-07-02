package fr.jdiot.dev.flux.codec;

import java.io.InputStream;

import fr.jdiot.dev.flux.exception.FluxException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import tools.jackson.databind.ObjectMapper;

/**
 * Jackson-based implementation of FluxCodec. Optimized for minimal memory
 * footprint and zero-copy where possible.
 */
public class JacksonFluxCodec<T> implements FluxCodec<T> {

  private final ObjectMapper objectMapper;
  private final Class<T> type;

  public JacksonFluxCodec(final ObjectMapper objectMapper, final Class<T> type) {
    this.objectMapper = objectMapper;
    this.type = type;
  }

  public JacksonFluxCodec(final Class<T> type) {
    this.objectMapper = new ObjectMapper();
    this.type = type;
  }

  @Override
  public ByteBuf encode(final T object) {
    try {
      final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
      try (ByteBufOutputStream out = new ByteBufOutputStream(buf)) {
        this.objectMapper.writeValue((java.io.OutputStream) out, object);
      } catch (final Exception inner) {
        buf.release();
        throw inner;
      }
      return buf;
    } catch (final Exception e) {
      throw new FluxException("Failed to encode object of type " + this.type.getName(), e);
    }
  }

  @Override
  public T decode(final ByteBuf buffer) {
    try {
      if (buffer.hasArray()) {
        // Zero-copy read if backed by an array
        final int offset = buffer.arrayOffset() + buffer.readerIndex();
        final int length = buffer.readableBytes();
        final T result = this.objectMapper.readValue(buffer.array(), offset, length, this.type);
        buffer.skipBytes(length);
        return result;
      } else {
        // Stream read for direct buffers
        try (InputStream is = new ByteBufInputStream(buffer)) {
          return this.objectMapper.readValue(is, this.type);
        }
      }
    } catch (final Exception e) {
      throw new FluxException("Failed to decode ByteBuf to " + this.type.getName(), e);
    }
  }
}
