package pg.hadoop.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Returns the hashcode of the given String or Number.
 */
public class HashCode extends EvalFunc<Integer> {

    @Override
    public Integer exec(final Tuple input) throws IOException {
        if (input.size() != 1) {
            throw new IllegalArgumentException("HashCode function requires a String or Number parameter.");
        }

        final Object obj = input.get(0);
        if (!(obj instanceof String) && !(obj instanceof Number)) {
            throw new IllegalArgumentException("HashCode only supports Strings and Numbers. Value=" + String.valueOf(obj));
        }
        return obj.hashCode();
    }
}
