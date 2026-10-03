package fr.jdiot.dev.flux.client;

import lombok.Getter;

@Getter
public class FluxClientProperties {
  private final int poolMaxConnections = 100;
  private final int poolPendingAcquireMaxCount = -1;
  private final int responseTimeoutMillis = 10_000;
  private final int initialWindowSize = 1048576 * 8;
  private final int maxFrameSize = 1048576; // 1MB

}
