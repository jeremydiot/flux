package fr.jdiot.dev.flux.config;

import lombok.Data;

/**
 * Configuration properties for chunk size, retries, and timeouts.
 */
@Data
public class FluxProperties {
  private int chunkSize = 8192;
  private int maxRetries = 3;
  private long connectionTimeoutMillis = 5000;
  private long readTimeoutMillis = 30000;
  private long writeTimeoutMillis = 30000;
  private int maxConnectionsPerHost = 500;
  private int poolSize = 32;
  private int backPressureSize = 256;
}
