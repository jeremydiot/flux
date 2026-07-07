package fr.jdiot.dev.flux;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import fr.jdiot.dev.flux.client.FluxClientImpl;
import fr.jdiot.dev.flux.client.FluxClientProperties;
import fr.jdiot.dev.flux.codec.AvroPojoCodec;
import fr.jdiot.dev.flux.codec.PojoCodec;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.core.Acknowledgement.Status;
import fr.jdiot.dev.flux.manager.FluxManager;
import fr.jdiot.dev.flux.manager.FluxManagerFactory;
import fr.jdiot.dev.flux.manager.FluxManagerProperties;
import fr.jdiot.dev.flux.server.FluxServerImpl;
import fr.jdiot.dev.flux.server.FluxServerProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;

public class FluxPushIT {

  private static FluxServerImpl server;
  private static FluxManager fluxManager;
  private static int port;
  private static DisposableServer disposableServer;

  @BeforeAll
  static void setUp() {
    final FluxManagerProperties properties = new FluxManagerProperties();
    properties.setBackPressureSize(256);

    // Real FluxManager, no Mockito spy
    FluxPushIT.fluxManager = FluxManagerFactory.create(properties);

    FluxPushIT.server = new FluxServerImpl("127.0.0.1", 0, new FluxServerProperties(), FluxPushIT.fluxManager);
    FluxPushIT.disposableServer = FluxPushIT.server.start();
    FluxPushIT.port = FluxPushIT.disposableServer.port();
  }

  @AfterAll
  static void tearDown() {
    if (FluxPushIT.server != null) {
      FluxPushIT.server.stop();
    }
  }

  @Test
  void testPushScenario5_2Push() throws InterruptedException {
    final String fluxId = "push-flux-it-456";

    final FluxClientImpl client = new FluxClientImpl("http://127.0.0.1:" + FluxPushIT.port, new FluxClientProperties());

    // 1. APP_CLIENT1 send chunked data flux to APP_SERVER.
    final Flux<ByteBuf> dataStream = Flux.just("PushA", "PushB").map(String::getBytes).map(Unpooled::wrappedBuffer);

    final Mono<Acknowledgement> pushAck = client.push(fluxId, dataStream);

    // Start pushing asynchronously
    final CountDownLatch ackLatch = new CountDownLatch(1);
    final Acknowledgement[] resultAck = new Acknowledgement[1];
    pushAck.subscribe(ack -> {
      resultAck[0] = ack;
      ackLatch.countDown();
    });

    // We must act as the server-side consumer to process the flux pushed by the
    // client.
    // If we don't consume it, the push might stall or never complete due to
    // backpressure.

    // In a real application, the server-side code (or another client) would call
    // getFlux().
    // We verify that the server receives exactly what the client sent.

    // We add a small delay to ensure the server had time to register the flux
    // when receiving the initial HTTP POST request from the client.
    Thread.sleep(200);

    StepVerifier.create(FluxPushIT.fluxManager.getFlux(fluxId).map(buf -> {
      final byte[] bytes = new byte[buf.readableBytes()];
      buf.readBytes(bytes);
      ReferenceCountUtil.safeRelease(buf);
      return new String(bytes);
    }).reduce("", String::concat)).expectNext("PushAPushB").verifyComplete();

    // 2. APP_SERVER return an acknowledge response for the reception of all chunked
    // data to APP_CLIENT1.
    // We verify that the client correctly received the SUCCESS acknowledgement.
    Assertions.assertTrue(ackLatch.await(2, TimeUnit.SECONDS), "Client did not receive the ACK in time");
    Assertions.assertNotNull(resultAck[0], "Acknowledgement should not be null");
    Assertions.assertEquals(Status.SUCCESS, resultAck[0].getStatus(), "Acknowledgement status should be SUCCESS");
    Assertions.assertEquals(fluxId, resultAck[0].getFluxId(), "Acknowledgement fluxId should match");
  }

  @Test
  void testPushHeaders() {
    final String fluxId = "push-flux-it-headers";

    final String[] transferEncoding = new String[1];
    final String[] contentType = new String[1];

    final DisposableServer dummyServer = HttpServer.create().protocol(HttpProtocol.H2C).host("127.0.0.1").port(0)
        .route(routes -> routes.post("/api/v1/flux/" + fluxId, (req, res) -> {
          transferEncoding[0] = req.requestHeaders().get("Transfer-Encoding");
          contentType[0] = req.requestHeaders().get("Content-Type");
          final PojoCodec<Acknowledgement> ackCodec = new AvroPojoCodec<>(Acknowledgement.class);
          final Acknowledgement ack = Acknowledgement.success(fluxId);
          final ByteBuf buf = ackCodec.encode(ack);
          final byte[] bytes = new byte[buf.readableBytes()];
          buf.readBytes(bytes);
          buf.release();
          return res.status(200).sendByteArray(Mono.just(bytes));
        })).bindNow();

    try {
      final FluxClientImpl client = new FluxClientImpl("http://127.0.0.1:" + dummyServer.port(),
          new FluxClientProperties());

      client.push(fluxId, Flux.just(new byte[] { 1, 2, 3 }).map(Unpooled::wrappedBuffer)).block();

      Assertions.assertEquals("chunked", transferEncoding[0]);
      Assertions.assertEquals("application/octet-stream", contentType[0]);
    } finally {
      dummyServer.disposeNow();
    }
  }
}
