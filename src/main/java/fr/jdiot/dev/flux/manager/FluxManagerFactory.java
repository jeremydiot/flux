package fr.jdiot.dev.flux.manager;

public class FluxManagerFactory {

  public static FluxManager create(final FluxManagerProperties properties) {
    return switch (properties.getBackpressureStrategy()) {
    case IN_MEMORY_BUFFER -> new BufferedFluxManagerImpl(properties);
    case TCP_LAZY -> new LazyFluxManagerImpl(properties);
    default -> throw new IllegalArgumentException("Unexpected value: " + properties.getBackpressureStrategy());
    };
  }

}
