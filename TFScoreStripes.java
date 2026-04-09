import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import opennlp.tools.stemmer.PorterStemmer;


public class TFScoreStripes {

    public static class TFMapper extends Mapper<Text, Text, Text, MapWritable> {

        private Map<String, Integer> dfMap = new HashMap<>();
        private Set<String> topTerms = new HashSet<>();
        private PorterStemmer stemmer = new PorterStemmer();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            for (URI uri : cacheFiles) {
                String name = new Path(uri.getPath()).getName();
                if (name == null || name.isEmpty()) continue;
                // Expect lines: TERM\tDF
                BufferedReader br = new BufferedReader(new FileReader(name));
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    String[] parts = line.split("\\s+");
                    String term = parts[0].toLowerCase();
                    int df = 1;
                    try {
                        df = Integer.parseInt(parts[parts.length - 1]);
                    } catch (Exception e) {
                        // fallback
                        df = 1;
                    }
                    term = stemmer.stem(term).toString();
                    dfMap.put(term, df);
                    topTerms.add(term);
                }
                br.close();
            }
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String docId = key.toString();
            String content = value.toString().toLowerCase();

            String[] tokens = content.split("\\W+");
            Map<String, Integer> tf = new HashMap<>();

            for (String token : tokens) {
                if (token == null) continue;
                token = token.trim();
                if (token.isEmpty()) continue;
                if (!token.matches("[a-zA-Z]+")) continue;
                String stem = stemmer.stem(token).toString();
                if (!topTerms.contains(stem)) continue;
                tf.put(stem, tf.getOrDefault(stem, 0) + 1);
            }

            if (tf.isEmpty()) return;

            MapWritable stripe = new MapWritable();
            for (Map.Entry<String, Integer> e : tf.entrySet()) {
                stripe.put(new Text(e.getKey()), new IntWritable(e.getValue()));
            }

            context.write(new Text(docId), stripe);
        }
    }

    public static class TFReducer extends Reducer<Text, MapWritable, Text, Text> {

        private Map<String, Integer> dfMap = new HashMap<>();
        private PorterStemmer stemmer = new PorterStemmer();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            for (URI uri : cacheFiles) {
                String name = new Path(uri.getPath()).getName();
                if (name == null || name.isEmpty()) continue;
                BufferedReader br = new BufferedReader(new FileReader(name));
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    String[] parts = line.split("\\s+");
                    String term = parts[0].toLowerCase();
                    int df = 1;
                    try {
                        df = Integer.parseInt(parts[parts.length - 1]);
                    } catch (Exception e) {
                        df = 1;
                    }
                    term = stemmer.stem(term).toString();
                    dfMap.put(term, df);
                }
                br.close();
            }
        }

        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> agg = new HashMap<>();

            for (MapWritable mw : values) {
                for (Map.Entry<Writable, Writable> entry : mw.entrySet()) {
                    String term = ((Text) entry.getKey()).toString();
                    int c = ((IntWritable) entry.getValue()).get();
                    agg.put(term, agg.getOrDefault(term, 0) + c);
                }
            }

            for (Map.Entry<String, Integer> e : agg.entrySet()) {
                String term = e.getKey();
                int tf = e.getValue();
                Integer dfObj = dfMap.get(term);
                if (dfObj == null) continue; // skip terms not found in DF cache
                int df = dfObj.intValue();
                if (df <= 0) df = 1;
                double score = tf * Math.log(10000.0 / (double) df + 1.0);
                String scoreStr = String.format("%.6f", score);
                context.write(new Text(key), new Text(term + "\t" + scoreStr));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: TFScoreStripes <input> <output> <top100_df.tsv>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "TF Score Stripes");
        job.setJarByClass(TFScoreStripes.class);

        // add DF cache file (only terms present in this file will be considered)
        job.addCacheFile(new Path(otherArgs[2]).toUri());

        // whole-file input format
        job.setInputFormatClass(WholeFileInputFormat.class);

        job.setMapperClass(TFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setReducerClass(TFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
