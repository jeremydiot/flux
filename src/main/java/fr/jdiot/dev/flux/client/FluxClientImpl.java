package fr.jdiot.dev.flux.client;

import java.time.Duration;

import fr.jdiot.dev.flux.codec.AvroPojoCodec;
import fr.jdiot.dev.flux.codec.PojoCodec;
import fr.jdiot.dev.flux.config.FluxProperties;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.exception.FluxException;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.client.HttpClient;

public class FluxClientImpl implements FluxClient {

  private final HttpClient httpClient;
  private final PojoCodec<Acknowledgement> ackCodec = new AvroPojoCodec<>(Acknowledgement.class);

  public FluxClientImpl(final String baseUrl, final FluxProperties properties) {
    this.httpClient = HttpClient.create().protocol(HttpProtocol.H2C).baseUrl(baseUrl)
        .responseTimeout(Duration.ofMillis(properties.getFluxTimeoutMillis()));
  }

  @Override
  public ByteBufFlux pull(final String fluxId) {
    return ByteBufFlux
        .fromInbound(this.httpClient.get().uri("/api/v1/flux/" + fluxId).responseConnection((res, connection) -> {
          if (res.status().code() >= 400) {
            return Flux.error(new FluxException("Failed to pull flux: " + res.status().code()));
          }
          return connection.inbound().receive().doOnComplete(() -> this.sendAck(Acknowledgement.success(fluxId)))
              .doOnError(_ -> this.sendAck(Acknowledgement.failed(fluxId)))
              .doOnCancel(() -> this.sendAck(Acknowledgement.partial(fluxId)));
        }));
  }

  private void sendAck(final Acknowledgement ack) {
    this.httpClient.headers(h -> h.add("Transfer-Encoding", "chunked").add("Content-Type", "application/octet-stream"))
        .post().uri("/api/v1/flux/" + ack.getFluxId() + "/ack").send(Mono.just(this.ackCodec.encode(ack))).response()
        .subscribe(_ -> {
          // NOOP
        }, err -> System.err.println("Failed to send ACK for fluxId " + ack.getFluxId() + ": " + err.getMessage()));
  }

  @Override
  public Mono<Acknowledgement> push(final String fluxId, final Flux<ByteBuf> dataStream) {
    return this.httpClient
        .headers(h -> h.add("Transfer-Encoding", "chunked").add("Content-Type", "application/octet-stream")).post()
        .uri("/api/v1/flux/" + fluxId).send(dataStream).responseSingle((res, byteBufMono) -> {
          if (res.status().code() >= 400) {
            return Mono.error(new FluxException("Failed to push flux: " + res.status().code()));
          }
          return byteBufMono.asByteArray().map(bytes -> this.ackCodec.decode(bytes));
        });
  }
}
