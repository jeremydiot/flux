package fr.jdiot.dev.flux.manager;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FluxManagerProperties {

  private int cleanupIntervalMillis = 10000;
  private int timeoutMillis = 60000;
  private BackpressureStrategy backpressureStrategy = BackpressureStrategy.TCP_LAZY;

  /**
   * Only for IN_MEMORY_BUFFER strategy, to limit the memory usage for buffer
   */
  private int backPressureSize = 256;

  public enum BackpressureStrategy {
    TCP_LAZY, IN_MEMORY_BUFFER
  }
}
