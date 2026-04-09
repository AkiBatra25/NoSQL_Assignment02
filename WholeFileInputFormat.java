import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WholeFileInputFormat extends FileInputFormat<Text, Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        WholeFileRecordReader reader = new WholeFileRecordReader();
        reader.initialize(split, context);
        return reader;
    }

    public static class WholeFileRecordReader extends RecordReader<Text, Text> {

        private FileSplit fileSplit;
        private Configuration conf;
        private boolean processed = false;
        private Text key = new Text();
        private Text value = new Text();

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {
            this.fileSplit = (FileSplit) genericSplit;
            this.conf = context.getConfiguration();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!processed) {
                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(conf);
                FSDataInputStream in = null;
                try {
                    in = fs.open(file);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    IOUtils.copyBytes(in, baos, 4096, false);
                    key.set(file.getName());
                    value.set(baos.toString("UTF-8"));
                } finally {
                    IOUtils.closeStream(in);
                }
                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {
            // nothing to do
        }
    }
}
