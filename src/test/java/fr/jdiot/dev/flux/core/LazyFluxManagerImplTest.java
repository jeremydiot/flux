package fr.jdiot.dev.flux.core;

import fr.jdiot.dev.flux.config.FluxProperties;

public class LazyFluxManagerImplTest extends AbstractFluxManagerTest {

  @Override
  protected FluxManager createFluxManager(final FluxProperties properties) {
    return new LazyFluxManagerImpl(properties);
  }

}
