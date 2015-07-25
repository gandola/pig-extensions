package pg.hadoop.pig.piggybank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.piggybank.storage.CSVExcelStorage;

import java.io.IOException;

/**
 * Piggybank CSVExcelStorage extension prepend the file path on the
 * very beginning of each tuple.
 *
 * @author Pedro Gandola <pedro.gandola@gmail.com>.
 */
public class CSVExcelStorageWithPath extends CSVExcelStorage {

    private Path path = null;

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        super.prepareToRead(reader, split);
        path = ((FileSplit) split.getWrappedSplit()).getPath();
    }

    @Override
    public Tuple getNext() throws IOException {
        final Tuple myTuple = super.getNext();
        if (myTuple != null) {
            myTuple.getAll().add(0, path.toString());
        }
        return myTuple;
    }
}
