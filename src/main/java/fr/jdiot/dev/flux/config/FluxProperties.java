package fr.jdiot.dev.flux.config;

import lombok.Data;

/**
 * Configuration properties for chunk size, retries, and timeouts.
 */
@Data
public class FluxProperties {
  private long fluxTimeoutMillis = 60000;
  private long fluxCleanupIntervalMillis = 10000;

  private int chunkSize = 8192;
  private int maxRetries = 3;
  private long connectionTimeoutMillis = 5000;
  private long writeTimeoutMillis = 30000;
  private int maxConnectionsPerHost = 500;
  private int poolSize = 32;
  private int backPressureSize = 256;

  private BackpressureStrategy backpressureStrategy = BackpressureStrategy.TCP_LAZY;

  public enum BackpressureStrategy {
    TCP_LAZY,
    IN_MEMORY_BUFFER
  }
}
