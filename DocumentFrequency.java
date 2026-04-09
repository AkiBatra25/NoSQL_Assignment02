import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

import opennlp.tools.stemmer.PorterStemmer;

public class DocumentFrequency {

    public static class DFMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Set<String> stopWords = new HashSet<>();
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private PorterStemmer stemmer = new PorterStemmer();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"));
                String line;
                while ((line = br.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
                br.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().toLowerCase().split("\\W+");

            Set<String> uniqueTerms = new HashSet<>();

            for (String token : tokens) {

                token = token.trim();

                if (token.isEmpty()) continue;
                if (!token.matches("[a-zA-Z]+")) continue;
                if (stopWords.contains(token)) continue;

                token = stemmer.stem(token).toString();

                uniqueTerms.add(token);
            }

            for (String term : uniqueTerms) {
                word.set(term);
                context.write(word, one);
            }
        }
    }

    public static class DFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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

        // 🔥 THIS LINE FIXES EVERYTHING
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: DocumentFrequency <input> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Document Frequency");

        job.setJarByClass(DocumentFrequency.class);

        job.setMapperClass(DFMapper.class);
        job.setReducerClass(DFReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new Path("stopwords.txt").toUri());

        // ✅ USE otherArgs instead of args
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}