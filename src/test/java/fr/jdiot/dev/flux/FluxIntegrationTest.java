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
import fr.jdiot.dev.flux.core.FluxManagerImpl;
import fr.jdiot.dev.flux.server.FluxServerImpl;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.test.StepVerifier;

public class FluxIntegrationTest {

  private static FluxServerImpl server;
  private static FluxManagerImpl fluxManager;
  private static int port;
  private static DisposableServer disposableServer;

  private static FluxProperties properties;

  private static ByteArrayFluxCodec dataCodec;

  @BeforeAll
  static void setUp() {
    FluxIntegrationTest.properties = new FluxProperties();
    FluxIntegrationTest.properties.setBackPressureSize(256);
    FluxIntegrationTest.properties.setReadTimeoutMillis(5000);

    FluxIntegrationTest.dataCodec = new ByteArrayFluxCodec();
    FluxIntegrationTest.fluxManager = org.mockito.Mockito.spy(new FluxManagerImpl(FluxIntegrationTest.properties));

    FluxIntegrationTest.server = new FluxServerImpl("127.0.0.1", 0, FluxIntegrationTest.properties,
        FluxIntegrationTest.fluxManager);
    FluxIntegrationTest.disposableServer = (DisposableServer) FluxIntegrationTest.server.start().block();
    FluxIntegrationTest.port = FluxIntegrationTest.disposableServer.port();
  }

  @AfterAll
  static void tearDown() {
    if (FluxIntegrationTest.server != null) {
      FluxIntegrationTest.server.stop();
    }
  }

  @Test
  void testPullScenario5_1Pull() throws InterruptedException {
    // APP_SERVER setup data flux
    final String fluxId = "pull-flux-123";
    final Flux<ByteBuf> testData = Flux.just("Chunk1", "Chunk2").map(String::getBytes).map(Unpooled::wrappedBuffer);

    FluxIntegrationTest.fluxManager.registerFlux(fluxId, testData).subscribe();

    // APP_CLIENT1 asking APP_SERVER to get data flux
    final FluxClientImpl<byte[]> client = new FluxClientImpl<>("http://127.0.0.1:" + FluxIntegrationTest.port,
        FluxIntegrationTest.properties, FluxIntegrationTest.dataCodec);

    StepVerifier.create(client.pull(fluxId).map(String::new).reduce("", String::concat)).expectNext("Chunk1Chunk2")
        .verifyComplete();

    // Verification of ACK by the server will happen automatically in FluxClientImpl
    // We use Mockito to wait and verify that the acknowledge method was called
    org.mockito.Mockito.verify(FluxIntegrationTest.fluxManager, org.mockito.Mockito.timeout(1000)).acknowledge(
        org.mockito.Mockito.eq(fluxId), org.mockito.Mockito.argThat(ack -> Status.SUCCESS.equals(ack.getStatus())));
  }

  @Test
  void testPushScenario5_2Push() {
    final String fluxId = "push-flux-456";
    final Flux<byte[]> clientData = Flux.just("Push1", "Push2").map(String::getBytes);
    final FluxClientImpl<byte[]> client = new FluxClientImpl<>("http://127.0.0.1:" + FluxIntegrationTest.port,
        FluxIntegrationTest.properties, FluxIntegrationTest.dataCodec);

    final Mono<Acknowledgement> ackMono = client.push(fluxId, clientData);

    StepVerifier.create(ackMono)
        .expectNextMatches(ack -> Status.SUCCESS.equals(ack.getStatus()) && fluxId.equals(ack.getFluxId()))
        .verifyComplete();
  }

  @Test
  void testBridgeScenario5_3Bridge() throws InterruptedException {
    final String fluxId = "bridge-flux-789";

    final FluxClientImpl<byte[]> client1 = new FluxClientImpl<>("http://127.0.0.1:" + FluxIntegrationTest.port,
        FluxIntegrationTest.properties, FluxIntegrationTest.dataCodec);
    final FluxClientImpl<byte[]> client2 = new FluxClientImpl<>("http://127.0.0.1:" + FluxIntegrationTest.port,
        FluxIntegrationTest.properties, FluxIntegrationTest.dataCodec);

    // Client 1 pulls - it will wait because no data yet
    final Flux<String> pullStream = client1.pull(fluxId).map(String::new);

    final CountDownLatch latch = new CountDownLatch(1);
    final List<String> results = new ArrayList<>();

    pullStream.subscribe(data -> results.add(data), _ -> latch.countDown(), () -> latch.countDown());

    // Wait a bit to ensure pull connection is established
    Thread.sleep(500);

    // Client 2 pushes
    final Flux<byte[]> pushData = Flux.just("Bridge1", "Bridge2").map(String::getBytes);
    final Mono<Acknowledgement> pushAck = client2.push(fluxId, pushData);

    // Verify Client 2 receives SUCCESS Ack from server AFTER Client 1 finishes
    StepVerifier.create(pushAck)
        .expectNextMatches(ack -> Status.SUCCESS.equals(ack.getStatus()) && fluxId.equals(ack.getFluxId()))
        .verifyComplete();

    // Wait for Client 1 pull to complete
    Assertions.assertTrue(latch.await(2, TimeUnit.SECONDS));
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals("Bridge1Bridge2", results.get(0));
  }
}
