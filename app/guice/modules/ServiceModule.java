package guice.modules;

import com.google.inject.AbstractModule;
import services.AzkabanApi;
import services.AzkabanApiImplimentation;

/**
 * Guice service module
 */
public class ServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AzkabanApi.class).to(AzkabanApiImplimentation.class);
    }
}
