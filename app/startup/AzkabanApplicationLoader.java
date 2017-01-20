package startup;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import guice.modules.ServiceModule;
import play.ApplicationLoader;
import play.inject.guice.GuiceApplicationBuilder;
import play.inject.guice.GuiceApplicationLoader;


/**
 * Custom application loader for the application to be used in application.conf
 *
 */
public class AzkabanApplicationLoader extends GuiceApplicationLoader {
    private final Config configuration;

    /**
     * Default constructor to load configurations
     */
    public AzkabanApplicationLoader() {
        this.configuration = ConfigFactory.load();
    }

    /**
     * Constructor for GuiceApplicationBuidler
     *
     * @param context
     * @return
     */
    @Override
    public GuiceApplicationBuilder builder(ApplicationLoader.Context context) {
        return initialBuilder
                .in(context.environment())
                .bindings(new ServiceModule());
    }
}
