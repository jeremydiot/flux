package fr.jdiot.dev.flux.codec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.core.Acknowledgement;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class AvroFluxCodecTest {

  private AvroFluxCodec<Acknowledgement> codec;

  @BeforeEach
  public void setUp() {
    this.codec = new AvroFluxCodec<>(Acknowledgement.class);
  }

  @Test
  public void testEncodeDecodeSingle() {
    final Acknowledgement ack = Acknowledgement.success("test-id");
    final ByteBuf buf = this.codec.encode(ack);

    final Acknowledgement decoded = this.codec.decode(buf);
    Assertions.assertEquals("test-id", decoded.getFluxId());
    Assertions.assertEquals(Acknowledgement.Status.SUCCESS, decoded.getStatus());
  }

  @Test
  public void testDecodeWithDirectBuffer() throws Exception {
    final Acknowledgement ack = Acknowledgement.failed("direct-id");
    
    // Encode to get the bytes
    final ByteBuf encodedBuf = this.codec.encode(ack);
    final byte[] bytes = new byte[encodedBuf.readableBytes()];
    encodedBuf.readBytes(bytes);
    encodedBuf.release();

    // Allocate direct buffer to test the InputStream logic
    final ByteBuf directBuf = Unpooled.directBuffer(bytes.length);
    directBuf.writeBytes(bytes);

    final Acknowledgement decoded = this.codec.decode(directBuf);
    Assertions.assertEquals("direct-id", decoded.getFluxId());
    Assertions.assertEquals(Acknowledgement.Status.FAILED, decoded.getStatus());

    directBuf.release();
  }

  @Test
  public void testEncodeDecodeFlux() {
    final Acknowledgement ack1 = Acknowledgement.success("id-1");
    final Acknowledgement ack2 = Acknowledgement.success("id-2");

    final Flux<Acknowledgement> inputFlux = Flux.just(ack1, ack2);

    final Flux<ByteBuf> encodedFlux = this.codec.encodeFlux(inputFlux);
    final Flux<Acknowledgement> decodedFlux = this.codec.decodeFlux(encodedFlux);

    StepVerifier.create(decodedFlux)
        .expectNextMatches(a -> "id-1".equals(a.getFluxId()) && Acknowledgement.Status.SUCCESS.equals(a.getStatus()))
        .expectNextMatches(a -> "id-2".equals(a.getFluxId()) && Acknowledgement.Status.SUCCESS.equals(a.getStatus()))
        .verifyComplete();
  }
}
