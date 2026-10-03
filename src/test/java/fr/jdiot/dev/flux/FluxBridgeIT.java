package fr.jdiot.dev.flux;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.client.FluxClientImpl;
import fr.jdiot.dev.flux.client.FluxClientProperties;
import fr.jdiot.dev.flux.core.ack.Acknowledgement;
import fr.jdiot.dev.flux.core.ack.Status;
import fr.jdiot.dev.flux.manager.FluxManager;
import fr.jdiot.dev.flux.manager.FluxManagerFactory;
import fr.jdiot.dev.flux.manager.FluxManagerProperties;
import fr.jdiot.dev.flux.manager.FluxManagerProperties.BackpressureStrategy;
import fr.jdiot.dev.flux.server.FluxServerImpl;
import fr.jdiot.dev.flux.server.FluxServerProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.test.StepVerifier;

@Slf4j
public class FluxBridgeIT {

  private static FluxServerImpl server;
  private static FluxManager fluxManager;
  private static int port;
  private static DisposableServer disposableServer;
  private static final List<Acknowledgement> interceptedAcks = new CopyOnWriteArrayList<>();
  private static FluxClientImpl client1;
  private static FluxClientImpl client2;

  @BeforeAll
  static void setUp() throws IOException {
    final FluxManagerProperties properties = new FluxManagerProperties();
    properties.setBackPressureSize(256);
    properties.setBackpressureStrategy(BackpressureStrategy.TCP_LAZY);

    // Real FluxManager, no Mockito spy
    FluxBridgeIT.fluxManager = FluxManagerFactory.create(properties);

    FluxBridgeIT.server = new FluxServerImpl("127.0.0.1", 0, new FluxServerProperties(), FluxBridgeIT.fluxManager);
    FluxBridgeIT.fluxManager.setAckHandler(FluxBridgeIT.interceptedAcks::add);
    FluxBridgeIT.disposableServer = FluxBridgeIT.server.start();
    FluxBridgeIT.port = FluxBridgeIT.disposableServer.port();

    FluxBridgeIT.client1 = new FluxClientImpl("http://127.0.0.1:" + FluxBridgeIT.port, new FluxClientProperties());
    FluxBridgeIT.client2 = new FluxClientImpl("http://127.0.0.1:" + FluxBridgeIT.port, new FluxClientProperties());

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

    // 1. APP_CLIENT1 asking APP_SERVER to get data flux.
    // 2. APP_SERVER keep open and save the connection with APP_CLIENT1, not respond
    // immediately.
    final Flux<ByteBuf> pullStream = FluxBridgeIT.client1.pull(fluxId);

    final CountDownLatch latch = new CountDownLatch(1);
    final List<String> results = new ArrayList<>();

    // Subscribe to trigger the pull, but do not block here.
    pullStream.reduce(new StringBuilder(), (sb, b) -> {
      final byte[] data = new byte[b.readableBytes()];
      b.readBytes(data);
      return sb.append(new String(data));
    }).map(StringBuilder::toString).subscribe(data -> results.add(data), _ -> latch.countDown(),
        () -> latch.countDown());

    // 3. APP_CLIENT2 send chunked data flux to APP_SERVER.
    final Flux<ByteBuf> fluxToPush = Flux.just("BridgeA", "BridgeB").map(String::getBytes).map(Unpooled::wrappedBuffer);

    final Mono<Acknowledgement> pushAck = FluxBridgeIT.client2.push(fluxId, fluxToPush);

    // 4 & 6. Verify Client 2 receives SUCCESS Ack from server AFTER Client 1
    // finishes and acknowledges.
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
