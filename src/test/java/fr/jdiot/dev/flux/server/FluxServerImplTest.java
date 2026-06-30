package fr.jdiot.dev.flux.server;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import fr.jdiot.dev.flux.codec.FluxCodec;
import fr.jdiot.dev.flux.codec.JacksonFluxCodec;
import fr.jdiot.dev.flux.config.FluxProperties;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.core.FluxManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.client.HttpClient;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectMapper;

public class FluxServerImplTest {

  private FluxServerImpl server;
  private FluxManager mockFluxManager;
  private FluxCodec<Acknowledgement> ackCodec;
  private DisposableServer disposableServer;

  @BeforeEach
  public void setUp() {
    this.mockFluxManager = Mockito.mock(FluxManager.class);
    this.ackCodec = new JacksonFluxCodec<>(new ObjectMapper(), Acknowledgement.class);
    final FluxProperties properties = new FluxProperties();

    this.server = new FluxServerImpl("localhost", 0, properties, this.mockFluxManager, this.ackCodec);
    this.disposableServer = this.server.start().block(Duration.ofSeconds(5));
  }

  @AfterEach
  public void tearDown() {
    if (this.server != null) {
      this.server.stop();
    }
  }

  @Test
  public void testPullFlux() {
    // Prepare mock data
    final ByteBuf data1 = Unpooled.copiedBuffer("chunk1", StandardCharsets.UTF_8);
    final ByteBuf data2 = Unpooled.copiedBuffer("chunk2", StandardCharsets.UTF_8);
    Mockito.when(this.mockFluxManager.getFlux("test-pull")).thenReturn(Flux.just(data1, data2));

    // Execute Request
    final HttpClient client = HttpClient.create().baseUrl("http://localhost:" + this.disposableServer.port());
    final Flux<String> response = client.get().uri("/api/v1/flux/test-pull").responseContent()
        .map(buf -> buf.toString(StandardCharsets.UTF_8));

    StepVerifier.create(response).expectNext("chunk1").expectNext("chunk2").verifyComplete();

    Mockito.verify(this.mockFluxManager).getFlux("test-pull");
  }

  @Test
  public void testPullFluxNotFound() {
    Mockito.when(this.mockFluxManager.getFlux("unknown-pull")).thenReturn(null);

    final HttpClient client = HttpClient.create().baseUrl("http://localhost:" + this.disposableServer.port());
    final Mono<Integer> responseCode = client.get().uri("/api/v1/flux/unknown-pull").response()
        .map(res -> res.status().code());

    StepVerifier.create(responseCode).expectNext(404).verifyComplete();

    Mockito.verify(this.mockFluxManager).getFlux("unknown-pull");
  }

  @Test
  public void testPushFlux() throws Exception {
    // Capture the flux that the server registers
    final ArgumentCaptor<Flux<ByteBuf>> fluxCaptor = ArgumentCaptor.forClass(Flux.class);
    final reactor.core.publisher.Sinks.One<Acknowledgement> ackSink = reactor.core.publisher.Sinks.one();
    Mockito.when(this.mockFluxManager.registerFlux(ArgumentMatchers.eq("test-push"), fluxCaptor.capture())).thenReturn(ackSink.asMono());

    final ByteBuf data1 = Unpooled.copiedBuffer("data1", StandardCharsets.UTF_8);

    final HttpClient client = HttpClient.create().baseUrl("http://localhost:" + this.disposableServer.port());

    final CompletableFuture<Acknowledgement> ackFuture = new CompletableFuture<>();

    client.headers(h -> h.add("X-Flux-Id", "test-push")).post().uri("/api/v1/flux").send(Flux.just(data1))
        .responseSingle((_, byteBufMono) -> byteBufMono).doOnNext(buf -> ackFuture.complete(this.ackCodec.decode(buf)))
        .doOnError(ackFuture::completeExceptionally).subscribe();

    // Wait a bit to ensure registration happened
    Mockito.verify(this.mockFluxManager, Mockito.timeout(1000)).registerFlux(ArgumentMatchers.eq("test-push"),
        ArgumentMatchers.any());

    // Now, emulate a puller consuming the flux from the FluxManager
    final Flux<ByteBuf> registeredFlux = fluxCaptor.getValue();
    StepVerifier.create(registeredFlux).expectNextMatches(buf -> "data1".equals(buf.toString(StandardCharsets.UTF_8)))
        .verifyComplete();

    ackSink.tryEmitValue(Acknowledgement.builder().fluxId("test-push").status("SUCCESS").build());

    // The request should now complete, and the server should have returned SUCCESS
    final Acknowledgement ack = ackFuture.get(2, TimeUnit.SECONDS);
    Assertions.assertEquals("test-push", ack.getFluxId());
    Assertions.assertEquals("SUCCESS", ack.getStatus());
  }

  @Test
  public void testPullAck() {
      Acknowledgement ack = Acknowledgement.builder()
              .fluxId("test-pull-ack")
              .status("SUCCESS")
              .build();

      HttpClient client = HttpClient.create().baseUrl("http://localhost:" + disposableServer.port());

      Mono<Void> requestComplete = client
              .post()
              .uri("/api/v1/flux/test-pull-ack/ack")
              .send(Flux.just(ackCodec.encode(ack)))
              .response()
              .then();

      StepVerifier.create(requestComplete)
              .verifyComplete();

      ArgumentCaptor<Acknowledgement> ackCaptor = ArgumentCaptor.forClass(Acknowledgement.class);
      Mockito.verify(mockFluxManager).acknowledge(ArgumentMatchers.eq("test-pull-ack"), ackCaptor.capture());
      
      Acknowledgement receivedAck = ackCaptor.getValue();
      Assertions.assertEquals("test-pull-ack", receivedAck.getFluxId());
      Assertions.assertEquals("SUCCESS", receivedAck.getStatus());
  }
}
