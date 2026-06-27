package fr.jdiot.dev.flux.client;

import java.time.Duration;

import fr.jdiot.dev.flux.codec.FluxCodec;
import fr.jdiot.dev.flux.config.FluxProperties;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.exception.FluxException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

public class FluxClientImpl<T> implements FluxClient<T> {

  private final HttpClient httpClient;
  private final FluxCodec<T> dataCodec;
  private final FluxCodec<Acknowledgement> ackCodec;

  public FluxClientImpl(final String baseUrl, final FluxProperties properties, final FluxCodec<T> dataCodec,
      final FluxCodec<Acknowledgement> ackCodec) {
    this.dataCodec = dataCodec;
    this.ackCodec = ackCodec;
    this.httpClient = HttpClient.create().baseUrl(baseUrl)
        .responseTimeout(Duration.ofMillis(properties.getReadTimeoutMillis()));
  }

  @Override
  public Flux<T> pull(final String fluxId) {
    return this.httpClient.get().uri("/api/v1/flux/" + fluxId).responseConnection((res, connection) -> {
      if (res.status().code() >= 400) {
        return Flux.error(new FluxException("Failed to pull flux: " + res.status().code()));
      }
      return this.dataCodec.decodeFlux(connection.inbound().receive());
    });
  }

  @Override
  public Mono<Acknowledgement> push(final String fluxId, final Flux<T> dataStream) {
    return this.httpClient
        .headers(h -> h.add("X-Flux-Id", fluxId).add("Transfer-Encoding", "chunked").add("Content-Type",
            "application/octet-stream"))
        .post().uri("/api/v1/flux").send(this.dataCodec.encodeFlux(dataStream)).responseSingle((res, byteBufMono) -> {
          if (res.status().code() >= 400) {
            return Mono.error(new FluxException("Failed to push flux: " + res.status().code()));
          }
          return byteBufMono.map(buf -> this.ackCodec.decode(buf));
        });
  }
}
