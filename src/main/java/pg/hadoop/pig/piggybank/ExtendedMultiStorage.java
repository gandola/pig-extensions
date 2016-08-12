package pg.hadoop.pig.piggybank;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.storage.MultiStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Extended MultiStorage class that allows to choose the fields to keep on the output file.
 */
public class ExtendedMultiStorage extends MultiStorage {

    private static Pattern COMMA_PATTERN = Pattern.compile(",");

    protected static final String DEFAULT_COMPRESSION = "none";
    protected static final String DEFAULT_DELIMITER = "\\t";

    protected int[] indexesToOutput;

    public ExtendedMultiStorage(String parentPathStr, String splitFieldIndex, String indexesToOutput) {
        this(parentPathStr, splitFieldIndex, DEFAULT_COMPRESSION, indexesToOutput);
    }

    public ExtendedMultiStorage(String parentPathStr, String splitFieldIndex, String compression, String indexesToOutput) {
        this(parentPathStr, splitFieldIndex, compression, DEFAULT_DELIMITER, indexesToOutput);
    }

    public ExtendedMultiStorage(String parentPathStr, String splitFieldIndex, String compression, String fieldDel, String indexesToOutput) {
        super(parentPathStr, splitFieldIndex, compression, fieldDel);
        if (indexesToOutput != null && !indexesToOutput.isEmpty()) {
            final String[] indexes = COMMA_PATTERN.split(indexesToOutput);
            if (indexes.length > 0) {
                this.indexesToOutput = new int[indexes.length];
                for (int i = 0; i < indexes.length; i++) {
                    this.indexesToOutput[i] = Integer.parseInt(indexes[i]);
                }
            }
        }
    }

    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        super.prepareToWrite(indexesToOutput == null ? writer : new RecordWriterWrapper(writer));
    }

    /**
     * Simple wrapper that intercepts the write call and remove the unnecessary field from the tuple before the write.
     */
    private class RecordWriterWrapper extends RecordWriter<String, Tuple> {

        private RecordWriter<String, Tuple> originalWriter;

        private RecordWriterWrapper(final RecordWriter<String, Tuple> originalWriter) {
            this.originalWriter = originalWriter;
        }

        @Override
        public void write(String key, Tuple value) throws IOException, InterruptedException {
            final List<Object> fields = new ArrayList<>();
            for (int indexToKeep : indexesToOutput) {
                fields.add(value.get(indexToKeep));
            }

            final Tuple newTuple = TupleFactory.getInstance().newTupleNoCopy(fields);
            originalWriter.write(key, newTuple);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            originalWriter.close(context);
        }
    }
}
