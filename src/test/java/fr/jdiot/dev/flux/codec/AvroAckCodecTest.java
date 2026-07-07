package fr.jdiot.dev.flux.codec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.exception.FluxException;
import io.netty.buffer.ByteBuf;

class AvroAckCodecTest {

  @Test
  void shouldEncodeAndDecodeSuccessfully() {
    final PojoCodec<Acknowledgement> codec = new AvroPojoCodec<>(Acknowledgement.class);
    final Acknowledgement ack = Acknowledgement.builder().fluxId("flux-123").status(Acknowledgement.Status.SUCCESS)
        .receivedChunks(10).totalBytes(1024L).timestamp("2023-10-27T10:00:00Z").reason("All good").build();

    final ByteBuf encoded = codec.encode(ack);
    Assertions.assertNotNull(encoded);
    Assertions.assertTrue(encoded.readableBytes() > 0);

    final byte[] bytes = new byte[encoded.readableBytes()];
    encoded.readBytes(bytes);
    encoded.release();

    final Acknowledgement decoded = codec.decode(bytes);
    Assertions.assertNotNull(decoded);
    Assertions.assertEquals("flux-123", decoded.getFluxId());
    Assertions.assertEquals(Acknowledgement.Status.SUCCESS, decoded.getStatus());
    Assertions.assertEquals(10, decoded.getReceivedChunks());
    Assertions.assertEquals(1024L, decoded.getTotalBytes());
    Assertions.assertEquals("2023-10-27T10:00:00Z", decoded.getTimestamp());
    Assertions.assertEquals("All good", decoded.getReason());
  }

  @Test
  void shouldHandleNullValues() {
    final PojoCodec<Acknowledgement> codec = new AvroPojoCodec<>(Acknowledgement.class);
    final Acknowledgement ack = Acknowledgement.success("flux-456");

    final ByteBuf encoded = codec.encode(ack);
    Assertions.assertNotNull(encoded);
    Assertions.assertTrue(encoded.readableBytes() > 0);

    final byte[] bytes = new byte[encoded.readableBytes()];
    encoded.readBytes(bytes);
    encoded.release();

    final Acknowledgement decoded = codec.decode(bytes);
    Assertions.assertNotNull(decoded);
    Assertions.assertEquals("flux-456", decoded.getFluxId());
    Assertions.assertEquals(Acknowledgement.Status.SUCCESS, decoded.getStatus());
    Assertions.assertEquals(0, decoded.getReceivedChunks());
    Assertions.assertEquals(0L, decoded.getTotalBytes());
    Assertions.assertNull(decoded.getTimestamp());
    Assertions.assertNull(decoded.getReason());
  }

  @Test
  void decodeShouldThrowFluxExceptionOnInvalidBytes() {
    final PojoCodec<Acknowledgement> codec = new AvroPojoCodec<>(Acknowledgement.class);
    final byte[] invalidBytes = new byte[] { 1, 2, 3, 4, 5 };

    Assertions.assertThrows(FluxException.class, () -> codec.decode(invalidBytes));
  }
}
