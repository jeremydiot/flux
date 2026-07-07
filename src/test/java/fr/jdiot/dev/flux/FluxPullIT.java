package fr.jdiot.dev.flux;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.client.FluxClientImpl;
import fr.jdiot.dev.flux.client.FluxClientProperties;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.core.Acknowledgement.Status;
import fr.jdiot.dev.flux.manager.FluxManager;
import fr.jdiot.dev.flux.manager.FluxManagerFactory;
import fr.jdiot.dev.flux.manager.FluxManagerProperties;
import fr.jdiot.dev.flux.server.FluxServerImpl;
import fr.jdiot.dev.flux.server.FluxServerProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.client.HttpClient;
import reactor.test.StepVerifier;

public class FluxPullIT {

  private static FluxServerImpl server;
  private static FluxManager fluxManager;
  private static int port;
  private static DisposableServer disposableServer;

  @BeforeAll
  static void setUp() {
    final FluxManagerProperties properties = new FluxManagerProperties();
    properties.setBackPressureSize(256);

    // Real FluxManager, no Mockito spy
    FluxPullIT.fluxManager = FluxManagerFactory.create(properties);

    FluxPullIT.server = new FluxServerImpl("127.0.0.1", 0, new FluxServerProperties(), FluxPullIT.fluxManager);
    FluxPullIT.disposableServer = FluxPullIT.server.start();
    FluxPullIT.port = FluxPullIT.disposableServer.port();
  }

  @AfterAll
  static void tearDown() {
    if (FluxPullIT.server != null) {
      FluxPullIT.server.stop();
    }
  }

  @Test
  void testPullScenario5_1Pull() throws InterruptedException {
    final String fluxId = "pull-flux-it-123";
    final Flux<ByteBuf> testData = Flux.just("ChunkA", "ChunkB").map(String::getBytes).map(Unpooled::wrappedBuffer);

    // 1. & 2. APP_SERVER setup data flux
    // registerFlux returns a Mono that will complete with the ACK when the client
    // acknowledges
    final Mono<Acknowledgement> serverAckMono = FluxPullIT.fluxManager.registerFlux(fluxId, testData);

    final CountDownLatch ackLatch = new CountDownLatch(1);
    final Acknowledgement[] receivedAck = new Acknowledgement[1];

    serverAckMono.subscribe(ack -> {
      receivedAck[0] = ack;
      ackLatch.countDown();
    });

    // 3. APP_CLIENT1 asking APP_SERVER to get data flux
    final FluxClientImpl client = new FluxClientImpl("http://127.0.0.1:" + FluxPullIT.port, new FluxClientProperties());

    // 1. APP_CLIENT1 asking APP_SERVER to get data flux.
    // 2. APP_SERVER return flux to APP_CLIENT1 with all chunked data.
    final Flux<ByteBuf> resultFlux = client.pull(fluxId);

    // We block to wait for the complete pull.
    final String results = resultFlux.reduce(new StringBuilder(), (sb, b) -> {
      final byte[] data = new byte[b.readableBytes()];
      b.readBytes(data);
      return sb.append(new String(data));
    }).map(StringBuilder::toString).block();

    Assertions.assertEquals("ChunkAChunkB", results);

    // The client should send the acknowledgement after successfully pulling all
    // data.
    // We verify the real server-side flux manager receives the success ACK.
    Assertions.assertTrue(ackLatch.await(2, TimeUnit.SECONDS), "Server did not receive the ACK in time");
    Assertions.assertNotNull(receivedAck[0], "Acknowledgement should not be null");
    Assertions.assertEquals(Status.SUCCESS, receivedAck[0].getStatus(), "Acknowledgement status should be SUCCESS");
    Assertions.assertEquals(fluxId, receivedAck[0].getFluxId(), "Acknowledgement fluxId should match");
  }

  @Test
  void testPullHeaders() {
    final String fluxId = "pull-flux-it-headers";
    final Flux<ByteBuf> testData = Flux.just("Chunk").map(String::getBytes).map(Unpooled::wrappedBuffer);
    FluxPullIT.fluxManager.registerFlux(fluxId, testData);

    StepVerifier.create(HttpClient.create().protocol(HttpProtocol.H2C).host("127.0.0.1").port(FluxPullIT.port).get()
        .uri("/api/v1/flux/" + fluxId).responseSingle((res, body) -> {
          Assertions.assertEquals("chunked", res.responseHeaders().get("Transfer-Encoding"));
          Assertions.assertEquals("application/octet-stream", res.responseHeaders().get("Content-Type"));
          return body.asString();
        })).expectNext("Chunk").verifyComplete();
  }
}
