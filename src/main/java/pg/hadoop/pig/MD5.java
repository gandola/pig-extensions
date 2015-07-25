package pg.hadoop.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Generates an MD5 in hexadecimal of a given string containing leading zero.
 *
 * @author Pedro Gandola <pedro.gandola@gmail.com>.
 */
public class MD5 extends EvalFunc<String> {

    private final MessageDigest messageDigest;

    public MD5() {
        try {
            messageDigest = MessageDigest.getInstance("md5");
        } catch (final NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public String exec(final Tuple input) throws IOException {
        if (input.size() != 1) {
            throw new IllegalArgumentException("MD5 function requires a String parameter.");
        }

        final Object obj = input.get(0);
        if (!(obj instanceof String)) {
            throw new IllegalArgumentException("MD5 function requires a String parameter.");
        }

        return toHex(messageDigest.digest(((String) obj).getBytes()));
    }

    private String toHex(byte[] data) {
        final StringBuilder buffer = new StringBuilder(data.length * 2);
        for (byte b : data) {
            buffer.append(Character.forDigit((b >> 4) & 0xF, 16));
            buffer.append(Character.forDigit((b & 0xF), 16));
        }
        return buffer.toString();
    }
}
