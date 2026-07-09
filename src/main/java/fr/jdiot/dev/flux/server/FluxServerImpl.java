package fr.jdiot.dev.flux.server;

import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Publisher;

import fr.jdiot.dev.flux.codec.AvroPojoCodec;
import fr.jdiot.dev.flux.codec.PojoCodec;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.manager.FluxManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import reactor.netty.http.server.HttpServerRoutes;

public class FluxServerImpl implements FluxServer {

  private final FluxServerProperties properties;
  private final FluxManager fluxManager;
  private final PojoCodec<Acknowledgement> ackCodec = new AvroPojoCodec<>(Acknowledgement.class);
  private final String host;
  private final int port;

  private DisposableServer disposableServer;

  public FluxServerImpl(final String host, final int port, final FluxServerProperties properties,
      final FluxManager fluxManager) {
    this.host = host;
    this.port = port;
    this.properties = properties;
    this.fluxManager = fluxManager;
  }

  @Override
  public DisposableServer start() {
    final HttpServer httpServer = HttpServer.create()
        .option(ChannelOption.SO_BACKLOG, this.properties.getInnerConnectionQueueSize())
        // .option(ChannelOption.SO_SNDBUF, 1024 * 1024) // doit être plus grand qu'un
        // chunk
        // .option(ChannelOption.SO_RCVBUF, 1024 * 1024) // doit être plus grand qu'un
        // chunk
        .childOption(ChannelOption.TCP_NODELAY, true).childOption(ChannelOption.SO_KEEPALIVE, true)
        .protocol(HttpProtocol.H2C).host(this.host).port(this.port).route(this::configureRoutes);

    httpServer.warmup().block();
    this.disposableServer = httpServer.bindNow();

    return this.disposableServer;
  }

  private void configureRoutes(final HttpServerRoutes routes) {
    routes.get("/api/v1/flux/{fluxId}", this::handlePullRequest);
    routes.post("/api/v1/flux/{fluxId}", this::handlePushRequest);
    routes.post("/api/v1/flux/{fluxId}/ack", this::handleAckRequest);
  }

  private Publisher<Void> handlePullRequest(final HttpServerRequest req, final HttpServerResponse res) {
    final String fluxId = req.param("fluxId");

    if (fluxId == null || fluxId.isEmpty()) {
      return res.status(400).sendString(Mono.just("Missing fluxId"));
    }

    final Flux<ByteBuf> dataStream = this.fluxManager.getFlux(fluxId);

    if (dataStream == null) {
      return res.status(404).sendString(Mono.just("Flux not found"));
    }

    return res.header("Content-Type", "application/octet-stream").send(dataStream);
  }

  private Publisher<Void> handlePushRequest(final HttpServerRequest req, final HttpServerResponse res) {
    final long t0 = System.nanoTime();

    final String fluxId = req.param("fluxId");

    if (fluxId == null || fluxId.isEmpty()) {
      return res.status(400).sendString(Mono.just("Missing fluxId"));
    }

    final AtomicLong t1 = new AtomicLong();
    final AtomicLong t2 = new AtomicLong();

    final Flux<ByteBuf> receiveStream = req.receive().doOnSubscribe(_ -> t1.set(System.nanoTime()))
        .map(ByteBuf::retain);

    final Mono<Acknowledgement> ackMono = this.fluxManager.registerFlux(fluxId, receiveStream).map(ack -> {
      t2.set(System.nanoTime());
      ack.setServerPreProcessingTimeMs((t1.get() - t0) / 1_000_000);
      ack.setServerProcessingTimeMs((t2.get() - t1.get()) / 1_000_000);
      return ack;
    });

    return res.header("Content-Type", "application/octet-stream").send(ackMono.map(ack -> {
      ack.setServerPostProcessingTimeMs((System.nanoTime() - t2.get()) / 1_000_000);
      return this.ackCodec.encode(ack);
    }));
  }

  private Publisher<Void> handleAckRequest(final HttpServerRequest req, final HttpServerResponse res) {
    final String fluxId = req.param("fluxId");
    if (fluxId == null || fluxId.isEmpty()) {
      return res.status(400).sendString(Mono.just("Missing fluxId"));
    }
    return res.send(req.receive().aggregate().asByteArray().flatMap(bytes -> {
      try {
        final Acknowledgement ack = this.ackCodec.decode(bytes);
        this.fluxManager.acknowledge(fluxId, ack);
        return Mono.empty();
      } catch (final Exception e) {
        return Mono.error(e);
      }
    }));
  }

  @Override
  public void stop() {
    if (this.disposableServer != null) {
      this.disposableServer.disposeNow();
    }
  }
}
