import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairsCooccurrenceInMapper {

    public static class PairMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Set<String> topWords = new HashSet<>();
        private Map<String, Integer> aggregationMap;
        private int d;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            d = conf.getInt("distance", 1);
            aggregationMap = new HashMap<>();

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

                for (int j = Math.max(0, i - d); j <= Math.min(tokens.length - 1, i + d); j++) {
                    if (i == j) continue;

                    String wj = tokens[j];

                    if (!topWords.contains(wj)) continue;
                    if (wi.equals(wj)) continue;

                    String pair = wi + "," + wj;
                    aggregationMap.put(pair, aggregationMap.getOrDefault(pair, 0) + 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text pairText = new Text();
            IntWritable count = new IntWritable();

            for (Map.Entry<String, Integer> entry : aggregationMap.entrySet()) {
                pairText.set(entry.getKey());
                count.set(entry.getValue());
                context.write(pairText, count);
            }
        }
    }

    public static class PairReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 3) {
            System.err.println("Usage: PairsCooccurrenceInMapper <in> <out> <distance>");
            System.exit(2);
        }
        conf.setInt("distance", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "Pairs Cooccurrence In-Mapper");
        job.addCacheFile(new Path("top50.txt").toUri());
        job.setJarByClass(PairsCooccurrenceInMapper.class);

        job.setMapperClass(PairMapper.class);
        job.setReducerClass(PairReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}