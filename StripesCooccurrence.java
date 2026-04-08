import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripesCooccurrence {

    public static class StripeMapper extends Mapper<Object, Text, Text, MapWritable> {

        private Set<String> topWords = new HashSet<>();
        private int d;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            d = conf.getInt("distance", 1);

            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader br = new BufferedReader(new FileReader("top50.txt"));
                String line;
                while ((line = br.readLine()) != null) {
                    String word = line.split("\\s+")[0];
                    topWords.add(word);
                }
                br.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().toLowerCase().split("\\W+");

            for (int i = 0; i < tokens.length; i++) {
                String wi = tokens[i];

                if (!topWords.contains(wi)) continue;

                MapWritable stripe = new MapWritable();

                for (int j = Math.max(0, i - d); j <= Math.min(tokens.length - 1, i + d); j++) {
                    if (i == j) continue;

                    String wj = tokens[j];

                    if (!topWords.contains(wj)) continue;
                    if (wi.equals(wj)) continue;

                    Text wjText = new Text(wj);
                    if (stripe.containsKey(wjText)) {
                        IntWritable count = (IntWritable) stripe.get(wjText);
                        count.set(count.get() + 1);
                    } else {
                        stripe.put(wjText, new IntWritable(1));
                    }
                }

                if (!stripe.isEmpty()) {
                    context.write(new Text(wi), stripe);
                }
            }
        }
    }

    public static class StripeReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {

            MapWritable finalStripe = new MapWritable();

            for (MapWritable val : values) {
                for (Map.Entry<Writable, Writable> entry : val.entrySet()) {
                    Text wjText = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();

                    if (finalStripe.containsKey(wjText)) {
                        IntWritable finalCount = (IntWritable) finalStripe.get(wjText);
                        finalCount.set(finalCount.get() + count.get());
                    } else {
                        finalStripe.put(new Text(wjText), new IntWritable(count.get()));
                    }
                }
            }

            context.write(key, finalStripe);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 3) {
            System.err.println("Usage: StripesCooccurrence <in> <out> <distance>");
            System.exit(2);
        }
        conf.setInt("distance", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "Stripes Cooccurrence");
        
        job.addCacheFile(new Path("top50.txt").toUri());
        
        job.setJarByClass(StripesCooccurrence.class);

        job.setMapperClass(StripeMapper.class);
        job.setReducerClass(StripeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}