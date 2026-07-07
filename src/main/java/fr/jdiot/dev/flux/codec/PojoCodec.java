package fr.jdiot.dev.flux.codec;

import io.netty.buffer.ByteBuf;

public interface PojoCodec<T> {
  ByteBuf encode(T object);

  T decode(byte[] bytes);
}
