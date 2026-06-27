package fr.jdiot.dev.flux.client;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.config.FluxProperties;
import fr.jdiot.dev.flux.core.Acknowledgement;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectMapper;

public class FluxClientImplTest {

  private static DisposableServer mockServer;
  private static FluxClient fluxClient;
  private static final ObjectMapper mapper = new ObjectMapper();

  @BeforeAll
  public static void setUp() {
    FluxClientImplTest.mockServer = HttpServer.create().port(0)
        .route(routes -> routes.get("/api/v1/flux/test-pull", (_, res) -> res.sendString(Flux.just("chunk1", "chunk2")))
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
            }))
        .bindNow();

    final FluxProperties properties = new FluxProperties();
    FluxClientImplTest.fluxClient = new FluxClientImpl("http://localhost:" + FluxClientImplTest.mockServer.port(),
        properties, FluxClientImplTest.mapper);
  }

  @AfterAll
  public static void tearDown() {
    if (FluxClientImplTest.mockServer != null) {
      FluxClientImplTest.mockServer.disposeNow();
    }
  }

  @Test
  public void testPull() {
    final Flux<String> result = FluxClientImplTest.fluxClient.pull("test-pull")
        .map(buf -> buf.toString(StandardCharsets.UTF_8));

    StepVerifier.create(result).expectNext("chunk1").expectNext("chunk2").verifyComplete();

  }

  @Test
  public void testPush() {
    final ByteBuf data = Unpooled.copiedBuffer("data1", StandardCharsets.UTF_8);
    final Flux<ByteBuf> dataStream = Flux.just(data);

    final Mono<Acknowledgement> result = FluxClientImplTest.fluxClient.push("test-push", dataStream);

    StepVerifier.create(result)
        .expectNextMatches(ack -> "test-push".equals(ack.getFluxId()) && "SUCCESS".equals(ack.getStatus()))
        .verifyComplete();
  }
}
