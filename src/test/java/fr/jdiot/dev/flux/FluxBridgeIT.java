package fr.jdiot.dev.flux;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.client.FluxClientImpl;
import fr.jdiot.dev.flux.codec.ByteArrayFluxCodec;
import fr.jdiot.dev.flux.config.FluxProperties;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.core.Acknowledgement.Status;
import fr.jdiot.dev.flux.core.FluxManager;
import fr.jdiot.dev.flux.core.FluxManagerFactory;
import fr.jdiot.dev.flux.server.FluxServerImpl;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.test.StepVerifier;

public class FluxBridgeIT {

  private static FluxServerImpl server;
  private static FluxManager fluxManager;
  private static int port;
  private static DisposableServer disposableServer;
  private static FluxProperties properties;
  private static ByteArrayFluxCodec dataCodec;

  @BeforeAll
  static void setUp() {
    FluxBridgeIT.properties = new FluxProperties();
    FluxBridgeIT.properties.setBackPressureSize(256);

    FluxBridgeIT.dataCodec = new ByteArrayFluxCodec();

    // Real FluxManager, no Mockito spy
    FluxBridgeIT.fluxManager = FluxManagerFactory.create(FluxBridgeIT.properties);

    FluxBridgeIT.server = new FluxServerImpl("127.0.0.1", 0, FluxBridgeIT.properties, FluxBridgeIT.fluxManager);
    FluxBridgeIT.disposableServer = FluxBridgeIT.server.start().block();
    FluxBridgeIT.port = FluxBridgeIT.disposableServer.port();
  }

  @AfterAll
  static void tearDown() {
    if (FluxBridgeIT.server != null) {
      FluxBridgeIT.server.stop();
    }
  }

  @Test
  void testBridgeScenario5_3Bridge() throws InterruptedException {
    final String fluxId = "bridge-flux-it-789";

    final FluxClientImpl<byte[]> client1 = new FluxClientImpl<>("http://127.0.0.1:" + FluxBridgeIT.port,
        FluxBridgeIT.properties, FluxBridgeIT.dataCodec);
    final FluxClientImpl<byte[]> client2 = new FluxClientImpl<>("http://127.0.0.1:" + FluxBridgeIT.port,
        FluxBridgeIT.properties, FluxBridgeIT.dataCodec);

    // 1. APP_CLIENT1 asking APP_SERVER to get data flux.
    // 2. APP_SERVER keep open and save the connection with APP_CLIENT1, not respond immediately.
    final Flux<String> pullStream = client1.pull(fluxId).map(String::new);

    final CountDownLatch latch = new CountDownLatch(1);
    final List<String> results = new ArrayList<>();

    // Subscribe to trigger the pull, but do not block here.
    pullStream.reduce("", String::concat).subscribe(data -> results.add(data), _ -> latch.countDown(), () -> latch.countDown());

    // Wait a bit to ensure the HTTP GET connection for the pull is fully established and waiting on the server.
    Thread.sleep(500);

    // 3. APP_CLIENT2 send chunked data flux to APP_SERVER.
    final Flux<byte[]> pushData = Flux.just("BridgeA", "BridgeB").map(String::getBytes);
    final Mono<Acknowledgement> pushAck = client2.push(fluxId, pushData);

    // 4 & 6. Verify Client 2 receives SUCCESS Ack from server AFTER Client 1 finishes and acknowledges.
    // Subscribing via StepVerifier starts the push operation.
    StepVerifier.create(pushAck)
        .expectNextMatches(ack -> Status.SUCCESS.equals(ack.getStatus()) && fluxId.equals(ack.getFluxId()))
        .verifyComplete();

    // Wait for Client 1 pull to complete and verify the results.
    Assertions.assertTrue(latch.await(3, TimeUnit.SECONDS), "Client 1 pull did not complete in time");
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals("BridgeABridgeB", results.get(0));
  }
}
