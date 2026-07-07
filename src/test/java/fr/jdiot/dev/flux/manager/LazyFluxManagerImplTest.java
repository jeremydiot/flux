package fr.jdiot.dev.flux.manager;

public class LazyFluxManagerImplTest extends AbstractFluxManagerTest {

  @Override
  protected FluxManager createFluxManager(final FluxManagerProperties properties) {
    return new LazyFluxManagerImpl(properties);
  }

}
