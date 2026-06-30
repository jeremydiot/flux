package fr.jdiot.dev.flux.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public class FluxManagerImpl implements FluxManager {

  private static class FluxState {
    final Sinks.Many<ByteBuf> dataSink = Sinks.many().unicast().onBackpressureBuffer();
    final Sinks.One<Acknowledgement> ackSink = Sinks.one();
  }

  private final Map<String, FluxState> activeFluxes = new ConcurrentHashMap<>();

  @Override
  public Mono<Acknowledgement> registerFlux(final String fluxId, final Flux<ByteBuf> dataStream) {
    final FluxState state = this.activeFluxes.computeIfAbsent(fluxId, _ -> new FluxState());

    dataStream.subscribe(
        state.dataSink::tryEmitNext,
        err -> {
          state.dataSink.tryEmitError(err);
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
      return this.activeFluxes.computeIfAbsent(fluxId, _ -> new FluxState()).dataSink.asFlux();
    }
    final FluxState state = this.activeFluxes.get(fluxId);
    return state != null ? state.dataSink.asFlux() : null;
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
