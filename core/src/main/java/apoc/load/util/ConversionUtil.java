package apoc.load.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConversionUtil {
    public static final String ERROR_VALUE = "__ERROR";
    public static final String ERR_PREFIX = "Error with key ";

    public final static class FailStrategyDeserializer extends UntypedObjectDeserializer {
        private final boolean validate;
        private final List<String> errorList = new ArrayList<>();

        public FailStrategyDeserializer(boolean validate, JavaType listType, JavaType mapType) {
            super(listType, mapType);
            this.validate = validate;
        }

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            try {
                // fallback to standard deserialization
                return super.deserialize(p, ctxt);
            } catch (IOException e) {
                final String errMsg = ERR_PREFIX + p.getParsingContext().getCurrentName() + "; The error is: " + e.getMessage();
                if (validate) {
                    errorList.add(errMsg);
                    return ERROR_VALUE;
                }
                throw new IOException(e);
            }
        }

        public List<String> getErrorList() {
            return errorList;
        }
    }

}
