package fr.jdiot.dev.flux.codec;

import fr.jdiot.dev.flux.core.Acknowledgement;
import io.netty.buffer.ByteBuf;

public interface AckCodec {
  ByteBuf encode(Acknowledgement object);

  Acknowledgement decode(byte[] bytes);
}
