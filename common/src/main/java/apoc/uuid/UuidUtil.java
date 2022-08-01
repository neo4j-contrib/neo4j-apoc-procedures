package apoc.uuid;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

public class UuidUtil {

    public static String fromHexToBase64(String hexUuid) {
        var uuid = UUID.fromString(hexUuid);
        return generateBase64Uuid(uuid);
    }

    public static String fromBase64ToHex(String base64Uuid) {
        if (base64Uuid == null) {
            throw new NullPointerException();
        }
        if (base64Uuid.isBlank()) {
            throw new IllegalStateException("Expected not empty UUID value");
        }

        String valueForConversion;
        // check if Base64 text ends with '==' already (Base64 alignment)
        if (base64Uuid.endsWith("==")) {
            if (base64Uuid.length() != 24) {
                throw new IllegalStateException("Invalid UUID length. Expected 24 characters");
            }
            valueForConversion = base64Uuid;
        } else {
            if (base64Uuid.length() != 22) {
                throw new IllegalStateException("Invalid UUID length. Expected 22 characters");
            }
            valueForConversion = base64Uuid + "==";
        }

        var buffer = Base64.getDecoder().decode(valueForConversion);

        // Generate UUID from 16 byte buffer
        long msb = 0L;
        for (int i=0; i < 8; ++i) {
            msb <<= 8;
            msb |= (buffer[i] & 0xFF);
        }
        var lsb = 0L;
        for (int i=8; i < 16; ++i) {
            lsb <<= 8;
            lsb |= (buffer[i] & 0xFF);
        }

        var uuid = new UUID(msb, lsb);
        return uuid.toString();
    }

    public static String generateBase64Uuid(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        var encoded = Base64.getEncoder().encodeToString(bb.array());
        return encoded.substring(0, encoded.length() - 2); // skip '==' alignment
    }
}
