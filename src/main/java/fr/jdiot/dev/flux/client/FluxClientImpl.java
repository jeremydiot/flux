package fr.jdiot.dev.flux.client;

import java.time.Duration;

import fr.jdiot.dev.flux.config.FluxProperties;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.exception.FluxException;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import tools.jackson.databind.ObjectMapper;

public class FluxClientImpl implements FluxClient {

  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;

  public FluxClientImpl(final String baseUrl, final FluxProperties properties, final ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
    this.httpClient = HttpClient.create().baseUrl(baseUrl)
        .responseTimeout(Duration.ofMillis(properties.getReadTimeoutMillis()));
  }

  @Override
  public Flux<ByteBuf> pull(final String fluxId) {
    return this.httpClient.get().uri("/api/v1/flux/" + fluxId).responseConnection((res, connection) -> {
      if (res.status().code() >= 400) {
        return Flux.error(new FluxException("Failed to pull flux: " + res.status().code()));
      }
      return connection.inbound().receive();
    });
  }

  @Override
  public Mono<Acknowledgement> push(final String fluxId, final Flux<ByteBuf> dataStream) {
    return this.httpClient
        .headers(h -> h.add("X-Flux-Id", fluxId).add("Transfer-Encoding", "chunked").add("Content-Type",
            "application/octet-stream"))
        .post().uri("/api/v1/flux").send(dataStream).responseSingle((res, byteBufMono) -> {
          if (res.status().code() >= 400) {
            return Mono.error(new FluxException("Failed to push flux: " + res.status().code()));
          }
          return byteBufMono.map(buf -> {
            try {
              final byte[] bytes = new byte[buf.readableBytes()];
              buf.readBytes(bytes);
              return this.objectMapper.readValue(bytes, Acknowledgement.class);
            } catch (final Exception e) {
              throw new FluxException("Failed to parse Acknowledgement", e);
            }
          });
        });
  }
}
