package apoc.export.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.neo4j.values.storable.DurationValue;

import java.io.IOException;

public class DurationValueSerializer extends JsonSerializer<DurationValue> {

    @Override
    public void serialize(DurationValue value, JsonGenerator jsonGenerator, SerializerProvider serializers) throws IOException {
        if (value == null) {
            jsonGenerator.writeNull();
        }
        jsonGenerator.writeString(value.toString());
    }
}

