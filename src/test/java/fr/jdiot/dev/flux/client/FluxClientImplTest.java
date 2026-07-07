package fr.jdiot.dev.flux.client;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.codec.AckCodec;
import fr.jdiot.dev.flux.codec.AvroAckCodec;
import fr.jdiot.dev.flux.codec.AvroFluxCodec;
import fr.jdiot.dev.flux.config.FluxProperties;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.core.Acknowledgement.Status;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.DisposableServer;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;

public class FluxClientImplTest {

  private static DisposableServer mockServer;
  private static FluxClient<String> fluxClient;
  private static final AckCodec ackCodec = new AvroAckCodec();
  private static final AvroFluxCodec<String> dataCodec = new AvroFluxCodec<>(String.class);
  private static final Sinks.Many<Acknowledgement> serverAcks = Sinks.many().replay().all();

  @BeforeAll
  public static void setUp() {
    FluxClientImplTest.mockServer = HttpServer
        .create().protocol(
            HttpProtocol.H2C)
        .port(0)
        .route(routes -> routes
            .get("/api/v1/flux/test-pull",
                (_, res) -> res.send(Flux.just(FluxClientImplTest.dataCodec.encode("chunk1")).concatWith(
                    Mono.delay(Duration.ofMillis(50)).map(_ -> FluxClientImplTest.dataCodec.encode("chunk2")))))
            .post("/api/v1/flux/{fluxId}", (req, res) -> {
              Assertions.assertEquals("chunked", req.requestHeaders().get("Transfer-Encoding"));
              Assertions.assertEquals("application/octet-stream", req.requestHeaders().get("Content-Type"));
              final String fluxId = req.param("fluxId");
              return req.receive().aggregate().asString().flatMap(_ -> {
                try {
                  final Acknowledgement ack = Acknowledgement.success(fluxId);
                  final ByteBuf buf = FluxClientImplTest.ackCodec.encode(ack);
                  final byte[] bytes = new byte[buf.readableBytes()];
                  buf.readBytes(bytes);
                  buf.release();
                  return res.sendByteArray(Mono.just(bytes)).then();
                } catch (final Exception e) {
                  return res.status(500).send().then();
                }
              });
            })
            .post("/api/v1/flux/{fluxId}/ack", (req, res) -> req.receive().aggregate().asByteArray().flatMap(bytes -> {
              try {
                final Acknowledgement ack = FluxClientImplTest.ackCodec.decode(bytes);
                FluxClientImplTest.serverAcks.tryEmitNext(ack);
              } catch (final Exception e) {
                // ignore
              }
              return res.status(200).send().then();
            })))
        .bindNow();

    final FluxProperties properties = new FluxProperties();

    FluxClientImplTest.fluxClient = new FluxClientImpl<>("http://localhost:" + FluxClientImplTest.mockServer.port(),
        properties, FluxClientImplTest.dataCodec);
  }

  @AfterAll
  public static void tearDown() {
    if (FluxClientImplTest.mockServer != null) {
      FluxClientImplTest.mockServer.disposeNow();
    }
  }

  @Test
  public void testPull() {
    final Flux<String> result = FluxClientImplTest.fluxClient.pull("test-pull");

    StepVerifier.create(result).expectNext("chunk1").expectNext("chunk2").verifyComplete();

    StepVerifier.create(FluxClientImplTest.serverAcks.asFlux().filter(a -> "test-pull".equals(a.getFluxId())))
        .expectNextMatches(ack -> Status.SUCCESS.equals(ack.getStatus())).thenCancel().verify(Duration.ofSeconds(2));
  }

  @Test
  public void testPush() {
    final Flux<String> dataStream = Flux.just("data1");

    final Mono<Acknowledgement> result = FluxClientImplTest.fluxClient.push("test-push", dataStream);

    StepVerifier.create(result)
        .expectNextMatches(ack -> "test-push".equals(ack.getFluxId()) && Status.SUCCESS.equals(ack.getStatus()))
        .verifyComplete();
  }
}
