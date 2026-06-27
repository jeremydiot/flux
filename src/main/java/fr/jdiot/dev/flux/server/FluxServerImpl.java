package fr.jdiot.dev.flux.server;

import fr.jdiot.dev.flux.codec.FluxCodec;
import fr.jdiot.dev.flux.config.FluxProperties;
import fr.jdiot.dev.flux.core.Acknowledgement;
import fr.jdiot.dev.flux.core.FluxManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

public class FluxServerImpl implements FluxServer {

  private final FluxProperties properties;
  private final FluxManager fluxManager;
  private final FluxCodec<Acknowledgement> ackCodec;
  private final String host;
  private final int port;

  private DisposableServer disposableServer;

  public FluxServerImpl(final String host, final int port, final FluxProperties properties,
      final FluxManager fluxManager, final FluxCodec<Acknowledgement> ackCodec) {
    this.host = host;
    this.port = port;
    this.properties = properties;
    this.fluxManager = fluxManager;
    this.ackCodec = ackCodec;
  }

  @Override
  public Mono<? extends DisposableServer> start() {
    return HttpServer.create().host(this.host).port(this.port)
        .route(this::configureRoutes)
        .bind()
        .doOnNext(server -> this.disposableServer = server);
  }

  private void configureRoutes(reactor.netty.http.server.HttpServerRoutes routes) {
    routes.get("/api/v1/flux/{fluxId}", this::handlePullRequest);
    routes.post("/api/v1/flux", this::handlePushRequest);
    routes.post("/api/v1/flux/{fluxId}/ack", this::handlePullAckRequest);
  }

  private org.reactivestreams.Publisher<Void> handlePullRequest(reactor.netty.http.server.HttpServerRequest req, reactor.netty.http.server.HttpServerResponse res) {
    final String fluxId = req.param("fluxId");
    if (fluxId == null || fluxId.isEmpty()) {
      return res.status(400).sendString(Mono.just("Missing fluxId"));
    }
    final Flux<ByteBuf> dataStream = this.fluxManager.getFlux(fluxId);
    if (dataStream == null) {
      // In scenario 5.3, if the flux is not yet present, FluxManager should ideally return a deferred Flux.
      // If it returns null, we must return 404 Not Found.
      return res.status(404).sendString(Mono.just("Flux not found"));
    }
    return res.header("Transfer-Encoding", "chunked")
        .header("Content-Type", "application/octet-stream")
        .send(dataStream);
  }

  private org.reactivestreams.Publisher<Void> handlePushRequest(reactor.netty.http.server.HttpServerRequest req, reactor.netty.http.server.HttpServerResponse res) {
    final String fluxId = req.requestHeaders().get("X-Flux-Id");
    if (fluxId == null || fluxId.isEmpty()) {
      return res.status(400).sendString(Mono.just("Missing X-Flux-Id header"));
    }

    final Sinks.One<ByteBuf> ackSink = Sinks.one();

    final Flux<ByteBuf> hookedStream = req.receive().doOnComplete(() -> {
      final Acknowledgement ack = Acknowledgement.builder().fluxId(fluxId).status("SUCCESS").build();
      ackSink.tryEmitValue(this.ackCodec.encode(ack));
    }).doOnError(_ -> {
      final Acknowledgement ack = Acknowledgement.builder().fluxId(fluxId).status("ERROR").build();
      ackSink.tryEmitValue(this.ackCodec.encode(ack));
    });

    this.fluxManager.registerFlux(fluxId, hookedStream);

    return res.header("Content-Type", "application/json").send(ackSink.asMono());
  }

  private org.reactivestreams.Publisher<Void> handlePullAckRequest(reactor.netty.http.server.HttpServerRequest req, reactor.netty.http.server.HttpServerResponse res) {
    final String fluxId = req.param("fluxId");
    if (fluxId == null || fluxId.isEmpty()) {
      return res.status(400).sendString(Mono.just("Missing fluxId"));
    }
    return res.send(req.receive().aggregate().asByteArray().flatMap(bytes -> {
      try {
        final Acknowledgement ack = this.ackCodec.decode(Unpooled.wrappedBuffer(bytes));
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
