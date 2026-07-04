package fr.jdiot.dev.flux.core;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import fr.jdiot.dev.flux.config.FluxProperties;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public class LazyFluxManagerImpl implements FluxManager {

  private final FluxProperties properties;

  private class FluxState {
    final Sinks.One<Flux<ByteBuf>> streamSink = Sinks.one();
    final Sinks.One<Acknowledgement> ackSink = Sinks.one();
    final long creationTimestamp = System.currentTimeMillis();
  }

  private final Map<String, FluxState> activeFluxes = new ConcurrentHashMap<>();
  private final Disposable cleanupTask;

  public LazyFluxManagerImpl(final FluxProperties properties) {
    this.properties = properties;
    this.cleanupTask = Flux.interval(Duration.ofMillis(properties.getFluxCleanupIntervalMillis()))
        .subscribe(_ -> this.clearBrokenFluxes());
  }

  private void clearBrokenFluxes() {
    final long now = System.currentTimeMillis();
    final long timeout = this.properties.getFluxTimeoutMillis();
    this.activeFluxes.entrySet().removeIf(entry -> {
      final FluxState state = entry.getValue();
      if (now - state.creationTimestamp > timeout) {
        state.streamSink.tryEmitError(new TimeoutException("Flux timed out"));
        state.ackSink.tryEmitValue(Acknowledgement.failed(entry.getKey(), "Flux timed out"));
        return true;
      }
      return false;
    });
  }

  public void stop() {
    this.cleanupTask.dispose();

    this.activeFluxes.forEach((fluxId, state) -> {
      state.streamSink.tryEmitError(new InterruptedException("Flux manager stopped"));
      state.ackSink.tryEmitValue(Acknowledgement.failed(fluxId, "Flux manager stopped"));
    });
    this.activeFluxes.clear();
  }

  /**
   * client push request or client push bridge request
   */
  @Override
  public Mono<Acknowledgement> registerFlux(final String fluxId, final Flux<ByteBuf> dataStream) {
    final FluxState state = this.activeFluxes.computeIfAbsent(fluxId, _ -> new FluxState());

    final Flux<ByteBuf> hookedStream = dataStream.doOnCancel(() -> {
      if (fluxId != null && fluxId.startsWith("push-")) {
        state.ackSink.tryEmitValue(Acknowledgement.partial(fluxId));
        this.activeFluxes.remove(fluxId);
      }
    }).doOnError(_ -> {
      if (fluxId != null && fluxId.startsWith("push-")) {
        state.ackSink.tryEmitValue(Acknowledgement.failed(fluxId));
        this.activeFluxes.remove(fluxId);
      }
    }).doOnComplete(() -> {
      if (fluxId != null && fluxId.startsWith("push-")) {
        state.ackSink.tryEmitValue(Acknowledgement.success(fluxId));
        this.activeFluxes.remove(fluxId);
      }
    });

    state.streamSink.tryEmitValue(hookedStream);

    // response to client push or client pull bridge
    return state.ackSink.asMono();
  }

  /**
   * client pull request or client pull bridge request
   */
  @Override
  public Flux<ByteBuf> getFlux(final String fluxId) {

    if (fluxId != null && fluxId.startsWith("bridge-")) {
      return this.activeFluxes.computeIfAbsent(fluxId, _ -> new FluxState()).streamSink.asMono().flatMapMany(f -> f)
          .doOnDiscard(ByteBuf.class, ReferenceCountUtil::safeRelease);
    }

    final FluxState state = this.activeFluxes.get(fluxId);
    return state != null
        ? state.streamSink.asMono().flatMapMany(f -> f).doOnDiscard(ByteBuf.class, ReferenceCountUtil::safeRelease)
        : null;
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
