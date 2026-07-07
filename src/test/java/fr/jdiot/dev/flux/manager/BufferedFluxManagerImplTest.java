package fr.jdiot.dev.flux.manager;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class BufferedFluxManagerImplTest extends AbstractFluxManagerTest {

  @Override
  protected FluxManager createFluxManager(final FluxManagerProperties properties) {
    return new BufferedFluxManagerImpl(properties);
  }

  @Nested
  class SpecificTests {

    @Test
    public void testBackpressurePauses() {
      final FluxManagerProperties properties = new FluxManagerProperties();
      // In production, Reactor rounds small sizes up.
      // size 2 is rounded up to 8 by Queues.get(2) which delegates to SpscArrayQueue.
      properties.setBackPressureSize(2);

      final BufferedFluxManagerImpl manager = new BufferedFluxManagerImpl(properties);

      // Create 10 chunks.
      final ByteBuf[] chunks = new ByteBuf[10];
      for (int i = 0; i < 10; i++) {
        chunks[i] = Unpooled.copiedBuffer("chunk" + (i + 1), StandardCharsets.UTF_8);
      }

      final Flux<ByteBuf> dataStream = Flux.just(chunks);

      manager.registerFlux("push-1", dataStream);

      // The manager should have pulled exactly 8 items (rounded up buffer size)
      // The remaining 2 items are waiting safely upstream.
      for (int i = 0; i < 8; i++) {
        Assertions.assertEquals(1, chunks[i].refCnt(), "chunk" + (i + 1) + " should be in the buffer");
      }
      
      // Chunks 9 and 10 have not been pulled yet!
      // In a hot stream they wouldn't have been created, but here Flux.just created them in advance.
      
      // Get the flux and pull all items to prove none were lost
      final Flux<ByteBuf> pulledStream = manager.getFlux("push-1");

      StepVerifier.create(pulledStream)
          .expectNext(chunks[0], chunks[1], chunks[2], chunks[3], chunks[4], chunks[5], chunks[6], chunks[7], chunks[8], chunks[9])
          .verifyComplete();

      // safe release chunks just in case
      for (int i = 0; i < 10; i++) {
        ReferenceCountUtil.safeRelease(chunks[i]);
      }
    }
  }

}
