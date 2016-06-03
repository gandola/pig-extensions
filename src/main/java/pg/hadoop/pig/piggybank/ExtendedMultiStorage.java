package pg.hadoop.pig.piggybank;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.storage.MultiStorage;

import java.io.IOException;
import java.util.List;

/**
 * Extended MultiStorage class that allows to remove the key field from the outputted files.
 */
public class ExtendedMultiStorage extends MultiStorage {


    protected static final String DEFAULT_COMPRESSION = "none";
    protected static final String DEFAULT_DELIMITER = "\\t";

    protected final int keyIndex;
    protected final boolean includeFieldIndex;

    public ExtendedMultiStorage(String parentPathStr, String splitFieldIndex, String includeFieldIndex) {
        this(parentPathStr, splitFieldIndex, DEFAULT_COMPRESSION, includeFieldIndex);
    }

    public ExtendedMultiStorage(String parentPathStr, String splitFieldIndex, String compression, String includeFieldIndex) {
        this(parentPathStr, splitFieldIndex, compression, DEFAULT_DELIMITER, includeFieldIndex);
    }

    public ExtendedMultiStorage(String parentPathStr, String splitFieldIndex, String compression, String fieldDel, String includeFieldIndex) {
        super(parentPathStr, splitFieldIndex, compression, fieldDel);
        this.keyIndex = Integer.parseInt(splitFieldIndex);
        this.includeFieldIndex = Boolean.parseBoolean(includeFieldIndex);
    }

    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        super.prepareToWrite(includeFieldIndex ? writer : new RecordWriterWrapper(writer));
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
            final List<Object> fields = value.getAll();
            fields.remove(keyIndex);
            final Tuple newTuple = TupleFactory.getInstance().newTupleNoCopy(fields);
            originalWriter.write(key, newTuple);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            originalWriter.close(context);
        }
    }
}
