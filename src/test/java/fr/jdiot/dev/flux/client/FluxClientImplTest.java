package fr.jdiot.dev.flux.client;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.codec.AvroPojoCodec;
import fr.jdiot.dev.flux.codec.PojoCodec;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.core.Acknowledgement.Status;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.DisposableServer;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;

public class FluxClientImplTest {

  private static DisposableServer mockServer;
  private static FluxClient fluxClient;
  private static final PojoCodec<Acknowledgement> ackCodec = new AvroPojoCodec<>(Acknowledgement.class);
  private static final Sinks.Many<Acknowledgement> serverAcks = Sinks.many().replay().all();

  @BeforeAll
  public static void setUp() {
    FluxClientImplTest.mockServer = HttpServer
        .create().protocol(
            HttpProtocol.H2C)
        .port(
            0)
        .route(routes -> routes
            .get("/api/v1/flux/test-pull", (_, res) -> res.send(
                Flux.just(Unpooled.wrappedBuffer("chunk1".getBytes()), Unpooled.wrappedBuffer("chunk2".getBytes()))))
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

    FluxClientImplTest.fluxClient = new FluxClientImpl("http://localhost:" + FluxClientImplTest.mockServer.port(),
        new FluxClientProperties());
  }

  @AfterAll
  public static void tearDown() {
    if (FluxClientImplTest.mockServer != null) {
      FluxClientImplTest.mockServer.disposeNow();
    }
  }

  @Test
  public void testPull() {
    final Flux<ByteBuf> result = FluxClientImplTest.fluxClient.pull("test-pull");

    StepVerifier.create(result.reduce(new StringBuilder(), (sb, b) -> {
      final byte[] data = new byte[b.readableBytes()];
      b.readBytes(data);

      return sb.append(new String(data));
    }).map(StringBuilder::toString)).expectNext("chunk1chunk2").verifyComplete();

    StepVerifier.create(FluxClientImplTest.serverAcks.asFlux().filter(a -> "test-pull".equals(a.getFluxId())))
        .expectNextMatches(ack -> Status.SUCCESS.equals(ack.getStatus())).thenCancel().verify(Duration.ofSeconds(2));
  }

  @Test
  public void testPush() {
    final Flux<ByteBuf> dataStream = Flux.just("data1").map(String::getBytes).map(Unpooled::wrappedBuffer);

    final Mono<Acknowledgement> result = FluxClientImplTest.fluxClient.push("test-push", dataStream);

    StepVerifier.create(result)
        .expectNextMatches(ack -> "test-push".equals(ack.getFluxId()) && Status.SUCCESS.equals(ack.getStatus()))
        .verifyComplete();
  }
}
