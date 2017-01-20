package startup;

import com.google.inject.Inject;
import play.inject.ApplicationLifecycle;

import java.util.concurrent.CompletableFuture;

/**
 * BootStrapper class which provides application startup functionality
 *
 */
public class AzkabanBootStrapper {
    /**
     * Constructor for AzkabanBootStrapper
     *
     */
    @Inject
    public AzkabanBootStrapper() {
        System.out.println("Application started successfully");
    }
}
