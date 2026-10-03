package fr.jdiot.dev.flux.server;

import lombok.Getter;

@Getter
public class FluxServerProperties {
  private final int innerConnectionQueueSize = 10000;
  private final int initialWindowSize = 1048576 * 8;
  private final int maxFrameSize = 1048576; // 1MB (max allowed by H2 spec is 16MB)
}
