package fr.jdiot.dev.flux.server;

import lombok.Getter;

@Getter
public class FluxServerProperties {
  private final int innerConnectionQueueSize = 10000;
  private final int initialWindowSize = 1048576 * 8;
}
