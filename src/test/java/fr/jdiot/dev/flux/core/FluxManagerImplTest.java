package fr.jdiot.dev.flux.core;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class FluxManagerImplTest {

  private FluxManagerImpl fluxManager;

  @BeforeEach
  public void setUp() {
    this.fluxManager = new FluxManagerImpl(new fr.jdiot.dev.flux.config.FluxProperties());
  }

  @Test
  public void testPushThenPull() {
    final ByteBuf chunk1 = Unpooled.copiedBuffer("chunk1", StandardCharsets.UTF_8);
    final ByteBuf chunk2 = Unpooled.copiedBuffer("chunk2", StandardCharsets.UTF_8);

    final Flux<ByteBuf> dataStream = Flux.just(chunk1, chunk2);
    final Mono<Acknowledgement> ackMono = this.fluxManager.registerFlux("bridge-1", dataStream);

    final Flux<ByteBuf> pulledStream = this.fluxManager.getFlux("bridge-1");

    StepVerifier.create(pulledStream)
        .expectNext(chunk1)
        .expectNext(chunk2)
        .verifyComplete();

    final Acknowledgement ack = Acknowledgement.builder().fluxId("bridge-1").status("SUCCESS").build();
    this.fluxManager.acknowledge("bridge-1", ack);

    StepVerifier.create(ackMono)
        .expectNext(ack)
        .verifyComplete();
  }

  @Test
  public void testPullThenPush() {
    final Flux<ByteBuf> pulledStream = this.fluxManager.getFlux("bridge-2");

    final ByteBuf chunk1 = Unpooled.copiedBuffer("chunk1", StandardCharsets.UTF_8);
    final Flux<ByteBuf> dataStream = Flux.just(chunk1);

    StepVerifier.create(pulledStream)
        .then(() -> {
            this.fluxManager.registerFlux("bridge-2", dataStream).subscribe();
        })
        .expectNext(chunk1)
        .verifyComplete();
  }
}
