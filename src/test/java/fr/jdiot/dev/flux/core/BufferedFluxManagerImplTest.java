package fr.jdiot.dev.flux.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.config.FluxProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class BufferedFluxManagerImplTest extends AbstractFluxManagerTest {

  @Override
  protected FluxManager createFluxManager(final FluxProperties properties) {
    return new BufferedFluxManagerImpl(properties);
  }

  @Nested
  class SpecificTests {

    @Test
    public void testBackpressureOverflow() {
      final FluxProperties properties = new FluxProperties();
      // In production, Reactor rounds small sizes up. 
      // size 2 is rounded up to 8 by Queues.get(2) which delegates to SpscArrayQueue.
      properties.setBackPressureSize(2);

      final BufferedFluxManagerImpl manager = new BufferedFluxManagerImpl(properties);

      // Create 10 chunks. The first 8 will fit in the queue, the last 2 will overflow.
      final ByteBuf[] chunks = new ByteBuf[10];
      for (int i = 0; i < 10; i++) {
        chunks[i] = Unpooled.copiedBuffer("chunk" + (i + 1), StandardCharsets.UTF_8);
      }

      final Flux<ByteBuf> dataStream = Flux.just(chunks);

      manager.registerFlux("push-1", dataStream);

      // Verify memory is safely released for the overflow chunks (chunks 9 and 10)
      for (int i = 0; i < 8; i++) {
        Assertions.assertEquals(1, chunks[i].refCnt(), "chunk" + (i + 1) + " should be in the buffer");
      }
      Assertions.assertEquals(0, chunks[8].refCnt(), "chunk9 should have been released due to overflow");
      Assertions.assertEquals(0, chunks[9].refCnt(), "chunk10 should have been released due to failure");

      // Get the flux and verify it fails with IllegalStateException after consuming the 8 buffered items
      final Flux<ByteBuf> pulledStream = manager.getFlux("push-1");

      StepVerifier.create(pulledStream, 0)
          .thenRequest(8)
          .expectNext(chunks[0], chunks[1], chunks[2], chunks[3], chunks[4], chunks[5], chunks[6], chunks[7])
          .expectErrorSatisfies(err -> {
            Assertions.assertTrue(err instanceof IllegalStateException);
            Assertions.assertEquals("Backpressure overflow", err.getMessage());
          }).verify(Duration.ofSeconds(1));

      // safe release chunks just in case
      for (int i = 0; i < 8; i++) {
        ReferenceCountUtil.safeRelease(chunks[i]);
      }
    }
  }

}
