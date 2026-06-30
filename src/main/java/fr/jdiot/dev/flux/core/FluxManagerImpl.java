package fr.jdiot.dev.flux.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import fr.jdiot.dev.flux.config.FluxProperties;

public class FluxManagerImpl implements FluxManager {

  private final FluxProperties properties;

  private class FluxState {
    final Sinks.Many<ByteBuf> dataSink;
    final Sinks.One<Acknowledgement> ackSink = Sinks.one();

    FluxState() {
      // Create a bounded unicast sink using the configured backpressure size
      this.dataSink = Sinks.many().unicast().onBackpressureBuffer(
          reactor.util.concurrent.Queues.<ByteBuf>get(properties.getBackPressureSize()).get());
    }
  }

  private final Map<String, FluxState> activeFluxes = new ConcurrentHashMap<>();

  public FluxManagerImpl(final FluxProperties properties) {
    this.properties = properties;
  }

  @Override
  public Mono<Acknowledgement> registerFlux(final String fluxId, final Flux<ByteBuf> dataStream) {
    final FluxState state = this.activeFluxes.computeIfAbsent(fluxId, _ -> new FluxState());

    dataStream.subscribe(
        buf -> {
          final Sinks.EmitResult result = state.dataSink.tryEmitNext(buf);
          if (result.isFailure()) {
            io.netty.util.ReferenceCountUtil.safeRelease(buf);
            state.dataSink.tryEmitError(new IllegalStateException("Backpressure overflow"));
          }
        },
        err -> {
          if (err != null) {
            state.dataSink.tryEmitError(err);
          }
          final Acknowledgement errorAck = Acknowledgement.builder().fluxId(fluxId).status("ERROR").build();
          state.ackSink.tryEmitValue(errorAck);
          this.activeFluxes.remove(fluxId);
        },
        () -> {
          state.dataSink.tryEmitComplete();
          // If it's not a bridge scenario, we automatically emit SUCCESS when the upload completes
          if (fluxId != null && !fluxId.startsWith("bridge-")) {
            final Acknowledgement successAck = Acknowledgement.builder().fluxId(fluxId).status("SUCCESS").build();
            state.ackSink.tryEmitValue(successAck);
          }
        }
    );

    return state.ackSink.asMono();
  }

  @Override
  public Flux<ByteBuf> getFlux(final String fluxId) {
    if (fluxId != null && fluxId.startsWith("bridge-")) {
      return this.activeFluxes.computeIfAbsent(fluxId, _ -> new FluxState()).dataSink.asFlux()
          .doOnDiscard(ByteBuf.class, io.netty.util.ReferenceCountUtil::safeRelease);
    }
    final FluxState state = this.activeFluxes.get(fluxId);
    return state != null ? state.dataSink.asFlux()
        .doOnDiscard(ByteBuf.class, io.netty.util.ReferenceCountUtil::safeRelease) : null;
  }

  @Override
  public void pauseFlux(final String fluxId) {
    // No-op for now
  }

  @Override
  public void resumeFlux(final String fluxId) {
    // No-op for now
  }

  @Override
  public void acknowledge(final String fluxId, final Acknowledgement ack) {
    final FluxState state = this.activeFluxes.remove(fluxId);
    if (state != null) {
      state.ackSink.tryEmitValue(ack);
    }
  }
}
