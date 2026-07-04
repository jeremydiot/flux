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
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

public class BufferedFluxManagerImpl implements FluxManager {

  private final FluxProperties properties;

  private class FluxState {
    final Sinks.Many<ByteBuf> dataSink;
    final Sinks.One<Acknowledgement> ackSink = Sinks.one();
    final long creationTimestamp = System.currentTimeMillis();

    FluxState(final int backPressureSize) {
      this.dataSink = Sinks.many().unicast().onBackpressureBuffer(Queues.<ByteBuf>get(backPressureSize).get());
    }
  }

  private final Map<String, FluxState> activeFluxes = new ConcurrentHashMap<>();
  private final Disposable cleanupTask;

  public BufferedFluxManagerImpl(final FluxProperties properties) {
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
        state.dataSink.tryEmitError(new TimeoutException("Flux timed out"));
        state.ackSink.tryEmitValue(Acknowledgement.failed(entry.getKey(), "Flux timed out"));
        return true;
      }
      return false;
    });
  }

  public void stop() {
    this.cleanupTask.dispose();

    this.activeFluxes.forEach((fluxId, state) -> {
      state.dataSink.tryEmitError(new InterruptedException("Flux manager stopped"));
      state.ackSink.tryEmitValue(Acknowledgement.failed(fluxId, "Flux manager stopped"));
    });
    this.activeFluxes.clear();
  }

  /**
   * client push request or client push bridge request
   */
  @Override
  public Mono<Acknowledgement> registerFlux(final String fluxId, final Flux<ByteBuf> dataStream) {
    final FluxState state = this.activeFluxes.computeIfAbsent(fluxId,
        _ -> new FluxState(this.properties.getBackPressureSize()));

    dataStream.subscribe(new BaseSubscriber<ByteBuf>() {
      @Override
      protected void hookOnNext(final ByteBuf buf) {
        final Sinks.EmitResult result = state.dataSink.tryEmitNext(buf);

        if (result.isFailure()) {
          ReferenceCountUtil.safeRelease(buf);
          state.dataSink.tryEmitError(new IllegalStateException("Backpressure overflow"));
        }
      }

      @Override
      protected void hookOnError(final Throwable err) {
        state.dataSink.tryEmitError(err != null ? err : new IllegalStateException("Unknown flux error"));
        if (fluxId != null && fluxId.startsWith("push-")) {
          state.ackSink.tryEmitValue(Acknowledgement.failed(fluxId));
        }
      }

      @Override
      protected void hookOnComplete() {
        state.dataSink.tryEmitComplete();
        if (fluxId != null && fluxId.startsWith("push-")) {
          state.ackSink.tryEmitValue(Acknowledgement.success(fluxId));
        }
      }

      @Override
      protected void hookOnCancel() {
        if (fluxId != null && fluxId.startsWith("push-")) {
          state.ackSink.tryEmitValue(Acknowledgement.partial(fluxId));
        }
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
      return this.activeFluxes.computeIfAbsent(fluxId,
          _ -> new FluxState(this.properties.getBackPressureSize())).dataSink.asFlux().doOnDiscard(ByteBuf.class,
              ReferenceCountUtil::safeRelease);
    }

    final FluxState state = this.activeFluxes.get(fluxId);
    if (state != null) {
      Flux<ByteBuf> flux = state.dataSink.asFlux().doOnDiscard(ByteBuf.class, ReferenceCountUtil::safeRelease);
      if (fluxId != null && fluxId.startsWith("push-")) {

        // remove here, backpressure consume push before pull call
        flux = flux.doFinally(_ -> this.activeFluxes.remove(fluxId));
      }
      return flux;
    }
    return null;
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
