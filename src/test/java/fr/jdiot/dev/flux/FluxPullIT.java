package fr.jdiot.dev.flux;

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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.test.StepVerifier;

public class FluxPullIT {

  private static FluxServerImpl server;
  private static FluxManager fluxManager;
  private static int port;
  private static DisposableServer disposableServer;
  private static FluxProperties properties;
  private static ByteArrayFluxCodec dataCodec;

  @BeforeAll
  static void setUp() {
    FluxPullIT.properties = new FluxProperties();
    FluxPullIT.properties.setBackPressureSize(256);

    FluxPullIT.dataCodec = new ByteArrayFluxCodec();

    // Real FluxManager, no Mockito spy
    FluxPullIT.fluxManager = FluxManagerFactory.create(FluxPullIT.properties);

    FluxPullIT.server = new FluxServerImpl("127.0.0.1", 0, FluxPullIT.properties, FluxPullIT.fluxManager);
    FluxPullIT.disposableServer = FluxPullIT.server.start().block();
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
    final FluxClientImpl<byte[]> client = new FluxClientImpl<>("http://127.0.0.1:" + FluxPullIT.port, FluxPullIT.properties, FluxPullIT.dataCodec);

    // Client pulls and we verify that chunk data is correctly received
    StepVerifier.create(client.pull(fluxId).map(String::new).reduce("", String::concat)).expectNext("ChunkAChunkB")
        .verifyComplete();

    // The client should send the acknowledgement after successfully pulling all
    // data.
    // We verify the real server-side flux manager receives the success ACK.
    Assertions.assertTrue(ackLatch.await(2, TimeUnit.SECONDS), "Server did not receive the ACK in time");
    Assertions.assertNotNull(receivedAck[0], "Acknowledgement should not be null");
    Assertions.assertEquals(Status.SUCCESS, receivedAck[0].getStatus(), "Acknowledgement status should be SUCCESS");
    Assertions.assertEquals(fluxId, receivedAck[0].getFluxId(), "Acknowledgement fluxId should match");
  }
}
