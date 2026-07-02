package fr.jdiot.dev.flux.core;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import fr.jdiot.dev.flux.config.FluxProperties;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

public class FluxManagerImpl implements FluxManager {

  private final FluxProperties properties;

  private class FluxState {
    final Sinks.Many<ByteBuf> dataSink;
    final Sinks.One<Acknowledgement> ackSink = Sinks.one();

    FluxState() {
      // Create a bounded unicast sink using the configured backpressure size
      this.dataSink = Sinks.many().unicast()
          .onBackpressureBuffer(Queues.<ByteBuf>get(FluxManagerImpl.this.properties.getBackPressureSize()).get());
    }
  }

  private final Map<String, FluxState> activeFluxes = new ConcurrentHashMap<>();

  public FluxManagerImpl(final FluxProperties properties) {
    this.properties = properties;
  }

  /**
   * client push request or client push bridge request
   */
  @Override
  public Mono<Acknowledgement> registerFlux(final String fluxId, final Flux<ByteBuf> dataStream) {
    final FluxState state = this.activeFluxes.computeIfAbsent(fluxId, _ -> new FluxState());

    dataStream.subscribe(buf -> {
      final Sinks.EmitResult result = state.dataSink.tryEmitNext(buf);

      if (result.isFailure()) {
        ReferenceCountUtil.safeRelease(buf);
        state.dataSink.tryEmitError(new IllegalStateException("Backpressure overflow"));
      }

    }, err -> {
      state.dataSink.tryEmitError(err != null ? err : new IllegalStateException("Unknown flux error"));
      if (fluxId != null && fluxId.startsWith("push-")) {
        state.ackSink.tryEmitValue(Acknowledgement.failed(fluxId));
        this.activeFluxes.remove(fluxId);
      }
    }, () -> {
      state.dataSink.tryEmitComplete();
      if (fluxId != null && fluxId.startsWith("push-")) {
        state.ackSink.tryEmitValue(Acknowledgement.success(fluxId));
        this.activeFluxes.remove(fluxId);
      }
    });

    // response to client push or client pull bridge
    return state.ackSink.asMono();
  }

  /**
   * client pull request or client pull bridge request
   */
  @Override
  public Flux<ByteBuf> getFlux(final String fluxId) {

    if (fluxId != null && fluxId.startsWith("bridge-")) {
      return this.activeFluxes.computeIfAbsent(fluxId, _ -> new FluxState()).dataSink.asFlux()
          .doOnDiscard(ByteBuf.class, ReferenceCountUtil::safeRelease);
    }

    final FluxState state = this.activeFluxes.get(fluxId);
    return state != null ? state.dataSink.asFlux().doOnDiscard(ByteBuf.class, ReferenceCountUtil::safeRelease) : null;
  }

  /**
   * client ack response after pull request
   */
  @Override
  public void acknowledge(final String fluxId, final Acknowledgement ack) {
    final FluxState state = this.activeFluxes.remove(fluxId);
    if (state != null) {
      state.ackSink.tryEmitValue(ack);
    }
  }

  @Override
  public Set<String> getActiveFluxIds() {
    return this.activeFluxes.keySet();
  }
}
