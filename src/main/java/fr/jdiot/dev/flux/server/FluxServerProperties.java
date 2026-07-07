package fr.jdiot.dev.flux.server;

import lombok.Getter;

@Getter
public class FluxServerProperties {
  private final int innerConnectionQueueSize = 10000;
}
