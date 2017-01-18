package errors;

import java.io.IOException;

/**
 * Exception to handle Azkaban API exceptions
 */
public class AzkabanApiException extends Exception{
    public AzkabanApiException() {
        super();
    }

    public AzkabanApiException(String message) {
        super(message);
    }

    public AzkabanApiException(String message, Throwable cause) {
        super(message, cause);
    }
}
