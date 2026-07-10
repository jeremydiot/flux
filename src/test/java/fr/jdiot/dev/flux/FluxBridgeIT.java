package fr.jdiot.dev.flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import fr.jdiot.dev.flux.codec.AvroPojoCodec;
import fr.jdiot.dev.flux.codec.PojoCodec;
import fr.jdiot.dev.flux.codec.SequentialFluxCodec;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.core.Acknowledgement.Status;
import fr.jdiot.dev.flux.core.FluxFile;
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

  @BeforeAll
  static void setUp() {
    final FluxManagerProperties properties = new FluxManagerProperties();
    properties.setBackPressureSize(256);
    properties.setBackpressureStrategy(BackpressureStrategy.TCP_LAZY);

    // Real FluxManager, no Mockito spy
    FluxBridgeIT.fluxManager = FluxManagerFactory.create(properties);

    FluxBridgeIT.server = new FluxServerImpl("127.0.0.1", 0, new FluxServerProperties(), FluxBridgeIT.fluxManager);
    FluxBridgeIT.fluxManager.setAckHandler(FluxBridgeIT.interceptedAcks::add);
    FluxBridgeIT.disposableServer = FluxBridgeIT.server.start();
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

    final FluxClientImpl client1 = new FluxClientImpl("http://127.0.0.1:" + FluxBridgeIT.port,
        new FluxClientProperties());
    final FluxClientImpl client2 = new FluxClientImpl("http://127.0.0.1:" + FluxBridgeIT.port,
        new FluxClientProperties());

    // 1. APP_CLIENT1 asking APP_SERVER to get data flux.
    // 2. APP_SERVER keep open and save the connection with APP_CLIENT1, not respond
    // immediately.
    final Flux<ByteBuf> pullStream = client1.pull(fluxId);

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

    final Mono<Acknowledgement> pushAck = client2.push(fluxId, fluxToPush);

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

  @Test
  void testBridgeScenario5_3BridgeWithFramedFileCodec() throws InterruptedException, IOException {
    final String fluxId = "bridge-flux-dicom-it-001";

    final FluxClientImpl client1 = new FluxClientImpl("http://127.0.0.1:" + FluxBridgeIT.port,
        new FluxClientProperties());
    final FluxClientImpl client2 = new FluxClientImpl("http://127.0.0.1:" + FluxBridgeIT.port,
        new FluxClientProperties());

    final PojoCodec<String> stringCodec = new AvroPojoCodec<>(String.class);
    final SequentialFluxCodec<String> framedCodec = new SequentialFluxCodec<>(stringCodec);

    // Prepare files to push
    final Path dicomDir = Paths.get("src/test/resources/dicom");

    // We read all files from the directory to test
    final List<FluxFile<String>> filesToPush = Collections.synchronizedList(new ArrayList<>());
    Files.list(dicomDir).filter(Files::isRegularFile).parallel().forEach(path -> {
      try {
        final byte[] data = Files.readAllBytes(path);
        filesToPush
            .add(FluxFile.<String>builder().metadata(path.getFileName().toString()).dataLength(data.length)
                .dataStream(Flux.range(0, (data.length + 65535) / 65536)
                    .map(i -> Unpooled.wrappedBuffer(data, i * 65536, Math.min(65536, data.length - i * 65536))))
                .build());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });

    final Flux<ByteBuf> pullStream = client1.pull(fluxId);

    final CountDownLatch latch = new CountDownLatch(1);
    final List<FluxFile<String>> results = new ArrayList<>();

    // Decode the pull stream
    final Flux<FluxFile<String>> decodedStream = framedCodec.decode(pullStream);

    decodedStream.concatMap(decodedFile -> decodedFile.getDataStream().reduce(0, (count, buf) -> {
      count += buf.readableBytes();
      buf.release();
      return count;
    }).map(count -> FluxFile.<String>builder().metadata(decodedFile.getMetadata()).dataLength(count)
        .dataStream(Flux.empty()).build())).subscribe(results::add, _ -> {
        }, () -> latch.countDown());



    final Flux<ByteBuf> fluxToPush = framedCodec.encode(Flux.fromStream(filesToPush.stream()));

    final long startTime = System.currentTimeMillis();

    final Mono<Acknowledgement> pushAck = client2.push(fluxId, fluxToPush)
        .doOnNext(ack -> FluxBridgeIT.log.info("\n{}", ack.printProcessingTimes()));

    StepVerifier.create(pushAck)
        .expectNextMatches(ack -> Status.SUCCESS.equals(ack.getStatus()) && fluxId.equals(ack.getFluxId()))
        .verifyComplete();

    Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "Client 1 pull did not complete in time");

    FluxBridgeIT.log.info("Total end-to-end bridge transfer time: {} ms", System.currentTimeMillis() - startTime);

    Assertions.assertEquals(filesToPush.size(), results.size());

    filesToPush.sort(Comparator.comparing(FluxFile::getMetadata));
    results.sort(Comparator.comparing(FluxFile::getMetadata));

    for (int i = 0; i < filesToPush.size(); i++) {
      FluxBridgeIT.log.debug("File {} sent metadata: {}, size: {}", i + 1, filesToPush.get(i).getMetadata(),
          filesToPush.get(i).getDataLength());
      FluxBridgeIT.log.debug("File {} received metadata: {}, size: {}", i + 1, results.get(i).getMetadata(),
          results.get(i).getDataLength());
      Assertions.assertEquals(filesToPush.get(i).getMetadata(), results.get(i).getMetadata());
      Assertions.assertEquals(filesToPush.get(i).getDataLength(), results.get(i).getDataLength());
    }

    // Verify that the server intercepted the success ack
    Assertions.assertTrue(
        FluxBridgeIT.interceptedAcks.stream()
            .anyMatch(ack -> fluxId.equals(ack.getFluxId()) && Status.SUCCESS.equals(ack.getStatus())),
        "Server should have intercepted the SUCCESS ack");
  }
}
