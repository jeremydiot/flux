package fr.jdiot.dev.flux.core;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.config.FluxProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class FluxManagerImplTest {

  private FluxManagerImpl fluxManager;

  @BeforeEach
  public void setUp() {
    this.fluxManager = new FluxManagerImpl(new FluxProperties());
  }

  @Test
  public void testBridgePullThenPush() {
    final ByteBuf chunk1 = Unpooled.copiedBuffer("chunk1", StandardCharsets.UTF_8);
    final ByteBuf chunk2 = Unpooled.copiedBuffer("chunk2", StandardCharsets.UTF_8);

    final Flux<ByteBuf> dataStream = Flux.just(chunk1, chunk2);

    // ! pull first
    final Flux<ByteBuf> pulledStream = this.fluxManager.getFlux("bridge-1");
    // ! push second
    final Mono<Acknowledgement> ackMono = this.fluxManager.registerFlux("bridge-1", dataStream);

    StepVerifier.create(pulledStream).expectNext(chunk1).expectNext(chunk2).verifyComplete();

    final Acknowledgement ack = Acknowledgement.success("bridge-1-ack");

    Assertions.assertEquals(1, this.fluxManager.getActiveFluxIds().size());

    this.fluxManager.acknowledge("bridge-1", ack);

    StepVerifier.create(ackMono).expectNext(ack).verifyComplete();

    Assertions.assertTrue(this.fluxManager.getActiveFluxIds().isEmpty());

  }

  @Test
  public void testBridgePushThenPull() {
    final ByteBuf chunk1 = Unpooled.copiedBuffer("chunk1", StandardCharsets.UTF_8);
    final ByteBuf chunk2 = Unpooled.copiedBuffer("chunk2", StandardCharsets.UTF_8);

    final Flux<ByteBuf> dataStream = Flux.just(chunk1, chunk2);

    // ! push first
    final Mono<Acknowledgement> ackMono = this.fluxManager.registerFlux("bridge-1", dataStream);
    // ! pull second
    final Flux<ByteBuf> pulledStream = this.fluxManager.getFlux("bridge-1");

    StepVerifier.create(pulledStream).expectNext(chunk1).expectNext(chunk2).verifyComplete();

    final Acknowledgement ack = Acknowledgement.success("bridge-1-ack");

    Assertions.assertEquals(1, this.fluxManager.getActiveFluxIds().size());

    this.fluxManager.acknowledge("bridge-1", ack);

    StepVerifier.create(ackMono).expectNext(ack).verifyComplete();

    Assertions.assertTrue(this.fluxManager.getActiveFluxIds().isEmpty());

  }

  @Test
  public void testPush() {
    final ByteBuf chunk1 = Unpooled.copiedBuffer("chunk1", StandardCharsets.UTF_8);
    final ByteBuf chunk2 = Unpooled.copiedBuffer("chunk2", StandardCharsets.UTF_8);

    final Flux<ByteBuf> dataStream = Flux.just(chunk1, chunk2);

    final Mono<Acknowledgement> ackMono = this.fluxManager.registerFlux("push-1", dataStream);

    StepVerifier.create(ackMono)
        .expectNextMatches(ack -> "push-1".equals(ack.getFluxId()) && Acknowledgement.Status.SUCCESS.equals(ack.getStatus()))
        .verifyComplete();

    Assertions.assertTrue(this.fluxManager.getActiveFluxIds().isEmpty());
  }


  @Test
  public void testClearBrokenFluxes() throws InterruptedException {
    final FluxProperties properties = new FluxProperties();
    properties.setFluxTimeoutMillis(5);
    properties.setFluxCleanupIntervalMillis(5);

    final FluxManagerImpl manager = new FluxManagerImpl(properties);

    // Register a flux
    manager.registerFlux("stale-flux", Flux.never());

    // Verify it's active
    Assertions.assertTrue(manager.getActiveFluxIds().contains("stale-flux"));

    // Wait for it to become stale
    Thread.sleep(20);

    // Verify it's removed
    Assertions.assertFalse(manager.getActiveFluxIds().contains("stale-flux"));
  }
}
