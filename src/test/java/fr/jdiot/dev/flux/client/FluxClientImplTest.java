package fr.jdiot.dev.flux.client;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.codec.JacksonFluxCodec;
import fr.jdiot.dev.flux.config.FluxProperties;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.core.Acknowledgement.Status;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectMapper;

public class FluxClientImplTest {

  private static DisposableServer mockServer;
  private static FluxClient<String> fluxClient;
  private static final ObjectMapper mapper = new ObjectMapper();

  @BeforeAll
  public static void setUp() {
    FluxClientImplTest.mockServer = HttpServer
        .create().protocol(reactor.netty.http.HttpProtocol.H2C).port(
            0)
        .route(routes -> routes
            .get("/api/v1/flux/test-pull",
                (_, res) -> res.sendString(
                    Flux.just("\"chunk1\"").concatWith(Mono.delay(Duration.ofMillis(50)).map(_ -> "\"chunk2\""))))
            .post("/api/v1/flux/{fluxId}", (req, res) -> {
              final String fluxId = req.param("fluxId");
              return req.receive().aggregate().asString().flatMap(_ -> {
                try {
                  final Acknowledgement ack = Acknowledgement.success(fluxId);
                  return res.sendString(Mono.just(FluxClientImplTest.mapper.writeValueAsString(ack))).then();
                } catch (final Exception e) {
                  return res.status(500).send().then();
                }
              });
            }).post("/api/v1/flux/{fluxId}/ack", (_, res) -> res.status(200).send()))
        .bindNow();

    final FluxProperties properties = new FluxProperties();
    final JacksonFluxCodec<String> dataCodec = new JacksonFluxCodec<>(FluxClientImplTest.mapper, String.class);

    FluxClientImplTest.fluxClient = new FluxClientImpl<>("http://localhost:" + FluxClientImplTest.mockServer.port(),
        properties, dataCodec);
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
