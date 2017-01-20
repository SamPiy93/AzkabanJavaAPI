package guice.modules;

import com.google.inject.AbstractModule;
import locks.ReEntrantReadWriteLockProvider;
import locks.ReadWriteLockProvider;
import startup.AzkabanBootStrapper;

/**
 * Guice contorller module
 */
public class ControllerModule extends AbstractModule{
    @Override
    protected void configure() {
        bind(ReadWriteLockProvider.class).to(ReEntrantReadWriteLockProvider.class);
        bind(AzkabanBootStrapper.class).asEagerSingleton();
    }
}
