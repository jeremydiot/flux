package fr.jdiot.dev.flux.core;

import fr.jdiot.dev.flux.config.FluxProperties;

public class FluxManagerFactory {

  public static FluxManager create(final FluxProperties properties) {
    return switch (properties.getBackpressureStrategy()) {
    case IN_MEMORY_BUFFER -> new BufferedFluxManagerImpl(properties);
    case TCP_LAZY -> new LazyFluxManagerImpl(properties);
    default -> throw new IllegalArgumentException("Unexpected value: " + properties.getBackpressureStrategy());
    };
  }

}
