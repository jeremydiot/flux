package fr.jdiot.dev.flux.core;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import fr.jdiot.dev.flux.config.FluxProperties;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public abstract class AbstractFluxManager implements FluxManager {

  protected final FluxProperties properties;

  private final Map<String, FluxState> activeFluxes = new ConcurrentHashMap<>();
  private final Disposable cleanupTask;

  private static class FluxState {
    final Sinks.One<Flux<ByteBuf>> streamSink = Sinks.one();
    final Sinks.One<Acknowledgement> ackSink = Sinks.one();
    final long creationTimestamp = System.currentTimeMillis();
  }

  protected AbstractFluxManager(final FluxProperties properties) {
    this.properties = properties;
    this.cleanupTask = Flux.interval(Duration.ofMillis(properties.getFluxCleanupIntervalMillis()))
        .subscribe(_ -> this.clearBrokenFluxes());
  }

  private void validateFluxId(final String fluxId) {
    if (fluxId == null || (!fluxId.startsWith("bridge-") && !fluxId.startsWith("push-") && !fluxId.startsWith("pull-"))) {
      throw new IllegalArgumentException("Invalid fluxId: must start with bridge-, push-, or pull-");
    }
  }

  /**
   * client pull request or client pull bridge request
   */
  @Override
  public Flux<ByteBuf> getFlux(final String fluxId) {
    this.validateFluxId(fluxId);

    final FluxState state = this.activeFluxes.computeIfAbsent(fluxId, _ -> new FluxState());
    Flux<ByteBuf> flux = state.streamSink.asMono().flatMapMany(f -> f)
        .doOnDiscard(ByteBuf.class, ReferenceCountUtil::safeRelease);

    if (fluxId.startsWith("push-")) {
      flux = flux.doFinally(_ -> this.activeFluxes.remove(fluxId));
    }

    return flux;
  }

  /**
   * client ack response after pull request
   */
  @Override
  public void acknowledge(final String fluxId, final Acknowledgement ack) {
    this.validateFluxId(fluxId);
    
    final FluxState state = this.activeFluxes.remove(fluxId);
    if (state != null) {
      state.ackSink.tryEmitValue(ack);
    }
  }

  @Override
  public Set<String> getActiveFluxIds() {
    return this.activeFluxes.keySet();
  }

  public void stop() {
    this.cleanupTask.dispose();

    this.activeFluxes.forEach((fluxId, state) -> {
      state.streamSink.tryEmitValue(Flux.error(new InterruptedException("Flux manager stopped")));
      state.ackSink.tryEmitValue(Acknowledgement.failed(fluxId, "Flux manager stopped"));
    });
    this.activeFluxes.clear();
  }

  protected Mono<Acknowledgement> internalRegisterFlux(final String fluxId, final Supplier<Flux<ByteBuf>> processor) {
    this.validateFluxId(fluxId);
    final FluxState state = this.activeFluxes.computeIfAbsent(fluxId, _ -> new FluxState());

    final Flux<ByteBuf> hookedFLux = processor.get().doOnCancel(() -> {
      if (fluxId != null && fluxId.startsWith("push-")) {
        state.ackSink.tryEmitValue(Acknowledgement.partial(fluxId));
      }
    }).doOnError(_ -> {
      if (fluxId != null && fluxId.startsWith("push-")) {
        state.ackSink.tryEmitValue(Acknowledgement.failed(fluxId));
      }
    }).doOnComplete(() -> {
      if (fluxId != null && fluxId.startsWith("push-")) {
        state.ackSink.tryEmitValue(Acknowledgement.success(fluxId));
      }
    });

    state.streamSink.tryEmitValue(hookedFLux);

    // response to client push or client pull bridge
    return state.ackSink.asMono();

  }

  private void clearBrokenFluxes() {
    final long now = System.currentTimeMillis();
    final long timeout = this.properties.getFluxTimeoutMillis();
    this.activeFluxes.forEach((fluxId, state) -> {
      if (now - state.creationTimestamp > timeout) {
        if (this.activeFluxes.remove(fluxId, state)) {
          state.streamSink.tryEmitValue(Flux.error(new TimeoutException("Flux timed out")));
          state.ackSink.tryEmitValue(Acknowledgement.failed(fluxId, "Flux timed out"));
        }
      }
    });
  }

}
