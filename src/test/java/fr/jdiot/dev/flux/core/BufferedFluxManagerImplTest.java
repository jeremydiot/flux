package fr.jdiot.dev.flux.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.config.FluxProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class BufferedFluxManagerImplTest {

  private BufferedFluxManagerImpl fluxManager;

  @BeforeEach
  public void setUp() {
    this.fluxManager = new BufferedFluxManagerImpl(new FluxProperties());
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

    // Subscribe to the flux to consume it so the stream completes and ack resolves
    final Flux<ByteBuf> pulledStream = this.fluxManager.getFlux("push-1");
    StepVerifier.create(pulledStream).expectNext(chunk1).expectNext(chunk2).verifyComplete();

    StepVerifier.create(ackMono)
        .expectNextMatches(
            ack -> "push-1".equals(ack.getFluxId()) && Acknowledgement.Status.SUCCESS.equals(ack.getStatus()))
        .verifyComplete();

    Assertions.assertTrue(this.fluxManager.getActiveFluxIds().isEmpty());
  }

  @Test
  public void testPull() {
    final ByteBuf chunk1 = Unpooled.copiedBuffer("chunk1", StandardCharsets.UTF_8);
    final ByteBuf chunk2 = Unpooled.copiedBuffer("chunk2", StandardCharsets.UTF_8);

    final Flux<ByteBuf> dataStream = Flux.just(chunk1, chunk2);

    final Mono<Acknowledgement> ackMono = this.fluxManager.registerFlux("pull-1", dataStream);

    // Consume the stream
    final Flux<ByteBuf> pulledStream = this.fluxManager.getFlux("pull-1");
    StepVerifier.create(pulledStream).expectNext(chunk1).expectNext(chunk2).verifyComplete();

    // In a pull scenario, stream completion does NOT automatically send an ACK.
    // We explicitly acknowledge it as a client would.
    this.fluxManager.acknowledge("pull-1", Acknowledgement.success("pull-1"));

    StepVerifier.create(ackMono)
        .expectNextMatches(
            ack -> "pull-1".equals(ack.getFluxId()) && Acknowledgement.Status.SUCCESS.equals(ack.getStatus()))
        .verifyComplete();

    Assertions.assertTrue(this.fluxManager.getActiveFluxIds().isEmpty());
  }

  @Test
  public void testRegisterFluxTimeout() {
    final FluxProperties properties = new FluxProperties();
    properties.setFluxTimeoutMillis(10);
    properties.setFluxCleanupIntervalMillis(5);

    final BufferedFluxManagerImpl manager = new BufferedFluxManagerImpl(properties);

    // Register a flux
    final Mono<Acknowledgement> ackMono = manager.registerFlux("stale-flux", Flux.never());

    // Verify it's active
    Assertions.assertTrue(manager.getActiveFluxIds().contains("stale-flux"));

    // StepVerifier will wait for the emission from the background cleanup task
    StepVerifier.create(ackMono)
        .expectNextMatches(
            ack -> "stale-flux".equals(ack.getFluxId()) && Acknowledgement.Status.FAILED.equals(ack.getStatus()))
        .expectComplete().verify(Duration.ofMillis(200));

    // Verify it's removed
    Assertions.assertFalse(manager.getActiveFluxIds().contains("stale-flux"));
  }

  @Test
  public void testGetFluxTimeout() {
    final FluxProperties properties = new FluxProperties();
    properties.setFluxTimeoutMillis(10);
    properties.setFluxCleanupIntervalMillis(5);

    final BufferedFluxManagerImpl manager = new BufferedFluxManagerImpl(properties);

    // Call getFlux for a bridge flux to create a pending streamSink
    final Flux<ByteBuf> pulledStream = manager.getFlux("bridge-1");

    Assertions.assertTrue(manager.getActiveFluxIds().contains("bridge-1"));

    // The stream should emit a TimeoutException when clearBrokenFluxes runs
    StepVerifier.create(pulledStream).expectError(TimeoutException.class).verify(Duration.ofMillis(200));

    Assertions.assertFalse(manager.getActiveFluxIds().contains("bridge-1"));
  }
}
