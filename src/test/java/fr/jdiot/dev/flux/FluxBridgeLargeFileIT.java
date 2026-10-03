package fr.jdiot.dev.flux;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;

import fr.jdiot.dev.flux.client.FluxClientImpl;
import fr.jdiot.dev.flux.client.FluxClientProperties;
import fr.jdiot.dev.flux.codec.AvroPojoCodec;
import fr.jdiot.dev.flux.codec.PojoCodec;
import fr.jdiot.dev.flux.codec.SequentialFluxCodec;
import fr.jdiot.dev.flux.core.AcknowledgementUtils;
import fr.jdiot.dev.flux.core.FluxFile;
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
public class FluxBridgeLargeFileIT {

  private static FluxServerImpl server;
  private static FluxManager fluxManager;
  private static int port;
  private static DisposableServer disposableServer;
  private static final List<Acknowledgement> interceptedAcks = new CopyOnWriteArrayList<>();
  private static FluxClientImpl client1;
  private static FluxClientImpl client2;

  private static List<DicomFile> largeFiles50MB;
  private static List<DicomFile> largeFiles1GB;
  private static List<DicomFile> files1330;

  @BeforeAll
  static void setUp() throws IOException {
    final FluxManagerProperties properties = new FluxManagerProperties();
    properties.setBackPressureSize(256);
    properties.setBackpressureStrategy(BackpressureStrategy.TCP_LAZY);

    FluxBridgeLargeFileIT.fluxManager = FluxManagerFactory.create(properties);

    FluxBridgeLargeFileIT.server = new FluxServerImpl("127.0.0.1", 0, new FluxServerProperties(),
        FluxBridgeLargeFileIT.fluxManager);
    FluxBridgeLargeFileIT.fluxManager.setAckHandler(FluxBridgeLargeFileIT.interceptedAcks::add);
    FluxBridgeLargeFileIT.disposableServer = FluxBridgeLargeFileIT.server.start();
    FluxBridgeLargeFileIT.port = FluxBridgeLargeFileIT.disposableServer.port();

    FluxBridgeLargeFileIT.client1 = new FluxClientImpl("http://127.0.0.1:" + FluxBridgeLargeFileIT.port,
        new FluxClientProperties());
    FluxBridgeLargeFileIT.client2 = new FluxClientImpl("http://127.0.0.1:" + FluxBridgeLargeFileIT.port,
        new FluxClientProperties());

    FluxBridgeLargeFileIT.largeFiles50MB = new ArrayList<>();
    FluxBridgeLargeFileIT.largeFiles1GB = new ArrayList<>();
    FluxBridgeLargeFileIT.files1330 = new ArrayList<>();

    final byte[] chunk = new byte[1024 * 1024]; // 1MB
    new Random().nextBytes(chunk);

    // Create 5 files of 50 MB
    FluxBridgeLargeFileIT.log.info("Generating 5 large files of 50MB each");
    for (int i = 0; i < 5; i++) {

      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        for (int j = 0; j < 50; j++) {
          baos.write(chunk);
        }

        FluxBridgeLargeFileIT.largeFiles50MB.add(new DicomFile(UUID.randomUUID().toString(), baos.toByteArray()));
      }
    }

    // Create 1 file of 1 GB
    FluxBridgeLargeFileIT.log.info("Generating 1 large file of 1GB");
    for (int i = 0; i < 1; i++) {

      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        for (int j = 0; j < 1024; j++) {
          baos.write(chunk);
        }
        FluxBridgeLargeFileIT.largeFiles1GB.add(new DicomFile(UUID.randomUUID().toString(), baos.toByteArray()));
      }
    }

    // Create 1330 files of 150KB
    final byte[] chunk150k = new byte[150 * 1024];
    new Random().nextBytes(chunk150k);
    FluxBridgeLargeFileIT.log.info("Generating 1330 files of 150KB each");
    for (int i = 0; i < 1330; i++) {
      FluxBridgeLargeFileIT.files1330.add(new DicomFile(UUID.randomUUID().toString(), chunk150k));
    }

    FluxBridgeLargeFileIT.log.info("Finished generating large files.");
  }

  @AfterAll
  static void tearDown() {
    if (FluxBridgeLargeFileIT.server != null) {
      FluxBridgeLargeFileIT.server.stop();
    }
  }

  @RepeatedTest(2)
  void testBridgeLargeFiles50MB() throws InterruptedException {
    final List<FluxFile<String>> filesToPush = this.getFilesToPush(FluxBridgeLargeFileIT.largeFiles50MB);

    this.runBridgeTest("bridge-flux-50mb-it", filesToPush);
  }

  @RepeatedTest(2)
  void testBridgeLargeFile1GB() throws InterruptedException {
    final List<FluxFile<String>> filesToPush = this.getFilesToPush(FluxBridgeLargeFileIT.largeFiles1GB);

    this.runBridgeTest("bridge-flux-1gb-it", filesToPush);
  }

  @RepeatedTest(2)
  void testBridgeFiles1330() throws InterruptedException {
    final List<FluxFile<String>> filesToPush = this.getFilesToPush(FluxBridgeLargeFileIT.files1330);
    this.runBridgeTest("bridge-flux-1330-it", filesToPush);
  }

  @RepeatedTest(2)
  void testBridgeLargeFiles50MBChunked() throws InterruptedException {
    final List<FluxFile<String>> filesToPush = this.getFilesToPushChunked(FluxBridgeLargeFileIT.largeFiles50MB);

    this.runBridgeTest("bridge-flux-50mb-it", filesToPush);
  }

  @RepeatedTest(2)
  void testBridgeLargeFile1GBChunked() throws InterruptedException {
    final List<FluxFile<String>> filesToPush = this.getFilesToPushChunked(FluxBridgeLargeFileIT.largeFiles1GB);

    this.runBridgeTest("bridge-flux-1gb-it", filesToPush);
  }

  @RepeatedTest(2)
  void testBridgeFiles1330Chunked() throws InterruptedException {
    final List<FluxFile<String>> filesToPush = this.getFilesToPushChunked(FluxBridgeLargeFileIT.files1330);
    this.runBridgeTest("bridge-flux-1330-it", filesToPush);
  }

  private void runBridgeTest(final String fluxId, final List<FluxFile<String>> filesToPush)
      throws InterruptedException {

    final PojoCodec<String> stringCodec = new AvroPojoCodec<>(String.class);
    final SequentialFluxCodec<String> framedCodec = new SequentialFluxCodec<>(stringCodec);

    // We read all files from the directory to test

    final Flux<ByteBuf> pullStream = FluxBridgeLargeFileIT.client1.pull(fluxId);

    final CountDownLatch latch = new CountDownLatch(1);
    final List<FluxFile<String>> results = new ArrayList<>();

    // Decode the pull stream
    final Flux<FluxFile<String>> decodedStream = framedCodec.decode(pullStream);

    decodedStream.flatMapSequential(decodedFile -> decodedFile.getDataStream().reduce(0, (count, buf) -> {
      count += buf.readableBytes();
      buf.release();
      return count;
    }).map(count -> FluxFile.<String>builder().metadata(decodedFile.getMetadata()).dataLength(count)
        .dataStream(Flux.empty()).build())).subscribe(results::add, _ -> {
        }, () -> latch.countDown());

    final Flux<ByteBuf> fluxToPush = framedCodec.encode(Flux.fromIterable(filesToPush));

    final long startTime = System.currentTimeMillis();

    final Mono<Acknowledgement> pushAck = FluxBridgeLargeFileIT.client2.push(fluxId, fluxToPush)
        .doOnNext(ack -> FluxBridgeLargeFileIT.log.info("\n{}", AcknowledgementUtils.pritableReport(ack)));

    StepVerifier.create(pushAck)
        .expectNextMatches(ack -> Status.SUCCESS.equals(ack.getStatus()) && fluxId.equals(ack.getFluxId()))
        .verifyComplete();

    Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "Client 1 pull did not complete in time");

    FluxBridgeLargeFileIT.log.info("Total end-to-end bridge transfer time: {} ms",
        System.currentTimeMillis() - startTime);

    Assertions.assertEquals(filesToPush.size(), results.size());

    filesToPush.sort(Comparator.comparing(FluxFile::getMetadata));
    results.sort(Comparator.comparing(FluxFile::getMetadata));

    for (int i = 0; i < filesToPush.size(); i++) {
      FluxBridgeLargeFileIT.log.debug("File {} sent metadata: {}, size: {}", i + 1, filesToPush.get(i).getMetadata(),
          filesToPush.get(i).getDataLength());
      FluxBridgeLargeFileIT.log.debug("File {} received metadata: {}, size: {}", i + 1, results.get(i).getMetadata(),
          results.get(i).getDataLength());
      Assertions.assertEquals(filesToPush.get(i).getMetadata(), results.get(i).getMetadata());
      Assertions.assertEquals(filesToPush.get(i).getDataLength(), results.get(i).getDataLength());
    }

    // Verify that the server intercepted the success ack
    Assertions.assertTrue(
        FluxBridgeLargeFileIT.interceptedAcks.stream()
            .anyMatch(ack -> fluxId.equals(ack.getFluxId()) && Status.SUCCESS.equals(ack.getStatus())),
        "Server should have intercepted the SUCCESS ack");
  }

  private List<FluxFile<String>> getFilesToPush(final List<DicomFile> dicomFiles) {
    final List<FluxFile<String>> filesToPush = Collections.synchronizedList(new ArrayList<>());
    dicomFiles.forEach(file -> {

      final byte[] data = file.data();
      filesToPush.add(FluxFile.<String>builder().metadata(file.name()).dataLength(data.length)
          .dataStream(Flux.just(Unpooled.wrappedBuffer(data))).build());

    });
    return filesToPush;
  }

  private List<FluxFile<String>> getFilesToPushChunked(final List<DicomFile> dicomFiles) {
    // We read all files from the directory to test, but we chunk the byte array
    final List<FluxFile<String>> filesToPush = Collections.synchronizedList(new ArrayList<>());
    dicomFiles.forEach(file -> {
      final byte[] data = file.data();
      final int chunkSize = 65536; // 64 KB

      filesToPush.add(FluxFile.<String>builder().metadata(file.name()).dataLength(data.length)
          .dataStream(Flux.generate(() -> 0, (state, sink) -> {
            if (state >= data.length) {
              sink.complete();
              return state;
            }
            final int length = Math.min(chunkSize, data.length - state);
            final byte[] chunkData = new byte[length];
            System.arraycopy(data, state, chunkData, 0, length);
            sink.next(Unpooled.wrappedBuffer(chunkData));
            return state + length;
          })).build());
    });
    return filesToPush;
  }

  private static record DicomFile(String name, byte[] data) {
  }
}
