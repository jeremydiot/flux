package fr.jdiot.dev.flux.client;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import fr.jdiot.dev.flux.codec.AvroPojoCodec;
import fr.jdiot.dev.flux.codec.PojoCodec;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.exception.FluxException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

public class FluxClientImpl implements FluxClient {

  private final HttpClient httpClient;
  private final PojoCodec<Acknowledgement> ackCodec = new AvroPojoCodec<>(Acknowledgement.class);

  public FluxClientImpl(final String baseUrl, final FluxClientProperties properties) {
    final ConnectionProvider provider = ConnectionProvider.builder("flux-client-pool")
        .maxConnections(properties.getPoolMaxConnections())
        .pendingAcquireMaxCount(properties.getPoolPendingAcquireMaxCount()).build();

    this.httpClient = HttpClient.create(provider).option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true).protocol(HttpProtocol.H2C).baseUrl(baseUrl)
        // .option(ChannelOption.SO_SNDBUF, 1024 * 1024) // doit être plus grand qu'un
        // chunk
        // .option(ChannelOption.SO_RCVBUF, 1024 * 1024) // doit être plus grand qu'un
        // chunk
        .responseTimeout(Duration.ofMillis(properties.getResponseTimeoutMillis()));

    this.httpClient.warmup().block(); // wait for client complete initialization
  }

  @Override
  public Flux<ByteBuf> pull(final String fluxId) {
    final AtomicLong t0 = new AtomicLong();
    final AtomicLong t1 = new AtomicLong();

    return this.httpClient.headers(h -> h.add("Accept", "application/octet-stream")).get().uri("/api/v1/flux/" + fluxId)
        .responseConnection((res, connection) -> {
          if (res.status().code() >= 400) {
            return Flux.error(new FluxException("Failed to pull flux: " + res.status().code()));
          }
          return connection.inbound().receive().doOnSubscribe(_ -> t1.set(System.nanoTime())).doOnComplete(() -> {
            final long t2 = System.nanoTime();
            final Acknowledgement ack = Acknowledgement.success(fluxId);
            ack.setPullClientPreProcessingTimeMs((t1.get() - t0.get()) / 1_000_000);
            ack.setPullClientProcessingTimeMs((t2 - t1.get()) / 1_000_000);
            this.sendAck(ack, t2);
          }).doOnError(_ -> {
            final long t2 = System.nanoTime();
            final Acknowledgement ack = Acknowledgement.failed(fluxId);
            ack.setPullClientPreProcessingTimeMs((t1.get() - t0.get()) / 1_000_000);
            ack.setPullClientProcessingTimeMs((t2 - t1.get()) / 1_000_000);
            this.sendAck(ack, t2);
          }).doOnCancel(() -> {
            final long t2 = System.nanoTime();
            final Acknowledgement ack = Acknowledgement.partial(fluxId);
            ack.setPullClientPreProcessingTimeMs((t1.get() - t0.get()) / 1_000_000);
            ack.setPullClientProcessingTimeMs((t2 - t1.get()) / 1_000_000);
            this.sendAck(ack, t2);
          });
        }).doOnSubscribe(_ -> t0.set(System.nanoTime()));
  }

  private void sendAck(final Acknowledgement ack, final long t2) {
    this.httpClient.headers(h -> h.add("Content-Type", "application/octet-stream")).post()
        .uri("/api/v1/flux/" + ack.getFluxId() + "/ack").send(Mono.fromCallable(() -> {
          ack.setPullClientPostProcessingTimeMs((System.nanoTime() - t2) / 1_000_000);
          return this.ackCodec.encode(ack);
        })).response().subscribe(_ -> {
          // NOOP
        }, err -> System.err.println("Failed to send ACK for fluxId " + ack.getFluxId() + ": " + err.getMessage()));
  }

  @Override
  public Mono<Acknowledgement> push(final String fluxId, final Flux<ByteBuf> dataStream) {
    final AtomicLong t0 = new AtomicLong();
    final AtomicLong t1 = new AtomicLong();
    final AtomicLong t2 = new AtomicLong();

    final Flux<ByteBuf> trackedStream = dataStream.doOnSubscribe(_ -> t1.set(System.nanoTime()))
        .doFinally(_ -> t2.set(System.nanoTime()));

    return this.httpClient
        .headers(h -> h.add("Content-Type", "application/octet-stream").add("Accept", "application/octet-stream"))
        .post().uri("/api/v1/flux/" + fluxId).send(trackedStream).responseSingle((res, content) -> {
          if (res.status().code() >= 400) {
            return Mono.error(new FluxException("Failed to push flux: " + res.status().code()));
          }
          return content.map(buf -> {
            final byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            buf.release();
            final Acknowledgement ack = this.ackCodec.decode(bytes);
            final long t3 = System.nanoTime();
            ack.setPushClientPreProcessingTimeMs((t1.get() - t0.get()) / 1_000_000);
            ack.setPushClientProcessingTimeMs((t2.get() - t1.get()) / 1_000_000);
            ack.setPushClientPostProcessingTimeMs((t3 - t2.get()) / 1_000_000);
            return ack;
          });
        }).doOnSubscribe(_ -> t0.set(System.nanoTime()));
  }
}
