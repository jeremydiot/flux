package fr.jdiot.dev.flux.codec;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class ByteArrayFluxCodecTest {

  private ByteArrayFluxCodec codec;

  @BeforeEach
  public void setUp() {
    this.codec = new ByteArrayFluxCodec();
  }

  @Test
  public void testEncodeDecodeSingle() {
    final byte[] original = "hello".getBytes(StandardCharsets.UTF_8);
    final ByteBuf buf = this.codec.encode(original);

    final byte[] decoded = this.codec.decode(buf);
    Assertions.assertArrayEquals(original, decoded);

    // Good practice to release buf if needed, though Unpooled.wrappedBuffer doesn't
    // strictly need it.
    buf.release();
  }

  @Test
  public void testEncodeDecodeFlux() {
    final byte[] chunk1 = "chunk1".getBytes(StandardCharsets.UTF_8);
    final byte[] chunk2 = "chunk2".getBytes(StandardCharsets.UTF_8);

    final Flux<byte[]> inputFlux = Flux.just(chunk1, chunk2);

    final Flux<ByteBuf> encodedFlux = this.codec.encodeFlux(inputFlux);
    final Flux<byte[]> decodedFlux = this.codec.decodeFlux(encodedFlux);

    StepVerifier.create(decodedFlux)
        .expectNextMatches(bytes -> "chunk1".equals(new String(bytes, StandardCharsets.UTF_8)))
        .expectNextMatches(bytes -> "chunk2".equals(new String(bytes, StandardCharsets.UTF_8))).verifyComplete();
  }
}
