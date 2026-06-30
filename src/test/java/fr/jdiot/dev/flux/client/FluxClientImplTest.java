package fr.jdiot.dev.flux.client;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.codec.JacksonFluxCodec;
import fr.jdiot.dev.flux.config.FluxProperties;
import fr.jdiot.dev.flux.core.Acknowledgement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectMapper;
import java.time.Duration;

public class FluxClientImplTest {

  private static DisposableServer mockServer;
  private static FluxClient<String> fluxClient;
  private static final ObjectMapper mapper = new ObjectMapper();

  @BeforeAll
  public static void setUp() {
    FluxClientImplTest.mockServer = HttpServer.create()
        .protocol(reactor.netty.http.HttpProtocol.H2C)
        .port(0)
        .route(routes -> routes
            .get("/api/v1/flux/test-pull", (_, res) -> res.sendString(Flux.just("\"chunk1\"").concatWith(Mono.delay(Duration.ofMillis(50)).map(_ -> "\"chunk2\""))))
            .post("/api/v1/flux", (req, res) -> {
              final String fluxId = req.requestHeaders().get("X-Flux-Id");
              return req.receive().aggregate().asString().flatMap(_ -> {
                try {
                  final Acknowledgement ack = Acknowledgement.builder().fluxId(fluxId).status("SUCCESS").build();
                  return res.sendString(Mono.just(FluxClientImplTest.mapper.writeValueAsString(ack))).then();
                } catch (final Exception e) {
                  return res.status(500).send().then();
                }
              });
            })
            .post("/api/v1/flux/{fluxId}/ack", (req, res) -> res.status(200).send()))
        .bindNow();

    final FluxProperties properties = new FluxProperties();
    final JacksonFluxCodec<String> dataCodec = new JacksonFluxCodec<>(FluxClientImplTest.mapper, String.class);
    final JacksonFluxCodec<Acknowledgement> ackCodec = new JacksonFluxCodec<>(FluxClientImplTest.mapper,
        Acknowledgement.class);
    FluxClientImplTest.fluxClient = new FluxClientImpl<>("http://localhost:" + FluxClientImplTest.mockServer.port(),
        properties, dataCodec, ackCodec);
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
        .expectNextMatches(ack -> "test-push".equals(ack.getFluxId()) && "SUCCESS".equals(ack.getStatus()))
        .verifyComplete();
  }
}
