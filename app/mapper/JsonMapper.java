package mapper;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import errors.PojoToJsonMappingException;

import java.io.IOException;

/**
 * Json mapper to map dtos
 */
public class JsonMapper {
    private static ObjectMapper objectMapper = new ObjectMapper().setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY)
            .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    private JsonMapper(){

    }

    public static <T> T mapJson(String requestJson, Class<T> type) throws PojoToJsonMappingException {
        try {
            return objectMapper.readValue(requestJson, type);
        } catch (IOException e) {
            throw new PojoToJsonMappingException("Error occurred when mapping JSON to the model class."
                    + type.getName()
                    + " JSON : "
                    + requestJson, e);
        }
    }
}
