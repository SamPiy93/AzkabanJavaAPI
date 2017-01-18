package errors;

import java.io.IOException;

/**
 * Exception to handle Json mapping
 */
public class PojoToJsonMappingException extends Exception{
    public PojoToJsonMappingException() {
        super();
    }

    public PojoToJsonMappingException(String message) {
        super(message);
    }

    public PojoToJsonMappingException(String message, Throwable cause) {
        super(message, cause);
    }
}
