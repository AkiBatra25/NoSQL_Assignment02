import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripesCooccurrenceCombiner {

    public static class StripeCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {

            MapWritable combinedStripe = new MapWritable();

            for (MapWritable val : values) {
                for (Map.Entry<Writable, Writable> entry : val.entrySet()) {
                    Text wjText = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();

                    if (combinedStripe.containsKey(wjText)) {
                        IntWritable existingCount = (IntWritable) combinedStripe.get(wjText);
                        existingCount.set(existingCount.get() + count.get());
                    } else {
                        combinedStripe.put(new Text(wjText), new IntWritable(count.get()));
                    }
                }
            }

            context.write(key, combinedStripe);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: StripesCooccurrenceCombiner <in> <out> <distance>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.setInt("distance", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "Stripes Cooccurrence with Combiner");

        job.addCacheFile(new Path("top50.txt").toUri());

        job.setJarByClass(StripesCooccurrenceCombiner.class);

        job.setMapperClass(StripesCooccurrence.StripeMapper.class);
        job.setCombinerClass(StripeCombiner.class);
        job.setReducerClass(StripesCooccurrence.StripeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}