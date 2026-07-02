package fr.jdiot.dev.flux.core;

import java.util.Map;
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
      final Throwable actualErr = err != null ? err : new IllegalStateException("Unknown flux error");
      state.dataSink.tryEmitError(actualErr);

      if (fluxId != null && fluxId.startsWith("push-")) {
        final Acknowledgement errorAck = Acknowledgement.failed(fluxId);
        state.ackSink.tryEmitValue(errorAck);
      }

      this.activeFluxes.remove(fluxId); // TODO
    }, () -> {
      state.dataSink.tryEmitComplete();
      // In a push scenario, we automatically emit SUCCESS when the upload completes.
      // In pull and bridge scenarios, we must wait for the client's explicit ACK.
      if (fluxId != null && fluxId.startsWith("push-")) {
        final Acknowledgement successAck = Acknowledgement.success(fluxId);
        state.ackSink.tryEmitValue(successAck);
      }
    });

    return state.ackSink.asMono();
  }

  @Override
  public Flux<ByteBuf> getFlux(final String fluxId) {

    if (fluxId != null && fluxId.startsWith("bridge-")) {
      return this.activeFluxes.computeIfAbsent(fluxId, _ -> new FluxState()).dataSink.asFlux()
          .doOnDiscard(ByteBuf.class, ReferenceCountUtil::safeRelease);
    }

    final FluxState state = this.activeFluxes.get(fluxId);
    return state != null ? state.dataSink.asFlux().doOnDiscard(ByteBuf.class, ReferenceCountUtil::safeRelease) : null;
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
