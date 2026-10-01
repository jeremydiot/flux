package fr.jdiot.dev.flux.codec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.core.AcknowledgementUtils;
import fr.jdiot.dev.flux.core.ack.Acknowledgement;
import fr.jdiot.dev.flux.core.ack.Status;
import fr.jdiot.dev.flux.exception.FluxException;
import io.netty.buffer.ByteBuf;

class AvroPojoCodecTest {

  @Test
  void shouldEncodeAndDecodeSuccessfully() {
    final PojoCodec<Acknowledgement> codec = new AvroPojoCodec<>(Acknowledgement.class);
    final Acknowledgement ack = new Acknowledgement("flux-123", Status.SUCCESS, 10, 1024L, 10L, 200L, 15L, 5L, 210L, 8L,
        2L, 250L, 3L, "All good");

    final ByteBuf encoded = codec.encode(ack);
    Assertions.assertNotNull(encoded);
    Assertions.assertTrue(encoded.readableBytes() > 0);

    final byte[] bytes = new byte[encoded.readableBytes()];
    encoded.readBytes(bytes);
    encoded.release();

    final Acknowledgement decoded = codec.decode(bytes);
    Assertions.assertNotNull(decoded);
    Assertions.assertEquals("flux-123", decoded.getFluxId());
    Assertions.assertEquals(Status.SUCCESS, decoded.getStatus());
    Assertions.assertEquals(10, decoded.getNbElement());
    Assertions.assertEquals(1024L, decoded.getTotalBytes());
    Assertions.assertEquals(10L, decoded.getServerPreProcessingTimeMs());
    Assertions.assertEquals(200L, decoded.getServerProcessingTimeMs());
    Assertions.assertEquals(15L, decoded.getServerPostProcessingTimeMs());
    Assertions.assertEquals(5L, decoded.getPullClientPreProcessingTimeMs());
    Assertions.assertEquals(210L, decoded.getPullClientProcessingTimeMs());
    Assertions.assertEquals(8L, decoded.getPullClientPostProcessingTimeMs());
    Assertions.assertEquals(2L, decoded.getPushClientPreProcessingTimeMs());
    Assertions.assertEquals(250L, decoded.getPushClientProcessingTimeMs());
    Assertions.assertEquals(3L, decoded.getPushClientPostProcessingTimeMs());
    Assertions.assertEquals("All good", decoded.getReason());
  }

  @Test
  void shouldHandleNullValues() {
    final PojoCodec<Acknowledgement> codec = new AvroPojoCodec<>(Acknowledgement.class);
    final Acknowledgement ack = AcknowledgementUtils.success("flux-456");

    final ByteBuf encoded = codec.encode(ack);
    Assertions.assertNotNull(encoded);
    Assertions.assertTrue(encoded.readableBytes() > 0);

    final byte[] bytes = new byte[encoded.readableBytes()];
    encoded.readBytes(bytes);
    encoded.release();

    final Acknowledgement decoded = codec.decode(bytes);
    Assertions.assertNotNull(decoded);
    Assertions.assertEquals("flux-456", decoded.getFluxId());
    Assertions.assertEquals(Status.SUCCESS, decoded.getStatus());
    Assertions.assertEquals(0, decoded.getNbElement());
    Assertions.assertEquals(0L, decoded.getTotalBytes());
    Assertions.assertEquals(0L, decoded.getServerPreProcessingTimeMs());
    Assertions.assertEquals(0L, decoded.getServerProcessingTimeMs());
    Assertions.assertEquals(0L, decoded.getServerPostProcessingTimeMs());
    Assertions.assertEquals(0L, decoded.getPullClientPreProcessingTimeMs());
    Assertions.assertEquals(0L, decoded.getPullClientProcessingTimeMs());
    Assertions.assertEquals(0L, decoded.getPullClientPostProcessingTimeMs());
    Assertions.assertEquals(0L, decoded.getPushClientPreProcessingTimeMs());
    Assertions.assertEquals(0L, decoded.getPushClientProcessingTimeMs());
    Assertions.assertEquals(0L, decoded.getPushClientPostProcessingTimeMs());
    Assertions.assertNull(decoded.getReason());
  }

  @Test
  void decodeShouldThrowFluxExceptionOnInvalidBytes() {
    final PojoCodec<Acknowledgement> codec = new AvroPojoCodec<>(Acknowledgement.class);
    final byte[] invalidBytes = new byte[] { 1, 2, 3, 4, 5 };

    Assertions.assertThrows(FluxException.class, () -> codec.decode(invalidBytes));
  }
}
