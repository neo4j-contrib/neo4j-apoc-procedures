package apoc.export.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.time.temporal.TemporalAccessor;

public class TemporalSerializer extends JsonSerializer<TemporalAccessor> {

    @Override
    public void serialize(TemporalAccessor value, JsonGenerator jsonGenerator, SerializerProvider serializers) throws IOException {
        if (value == null) {
            jsonGenerator.writeNull();
        }
        jsonGenerator.writeString(value.toString());
    }
}
