import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import opennlp.tools.stemmer.PorterStemmer;

public class WordCount2_2 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private PorterStemmer stemmer = new PorterStemmer();

        private boolean caseSensitive = false;
        private Set<String> stopWords = new HashSet<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);

            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = context.getCacheFiles();

                if (patternsURIs != null) {
                    for (URI patternsURI : patternsURIs) {
                        Path patternsPath = new Path(patternsURI.getPath());
                        String patternsFileName = patternsPath.getName();
                        parseSkipFile(patternsFileName);
                    }
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String pattern;
                while ((pattern = reader.readLine()) != null) {
                    stopWords.add(pattern.trim().toLowerCase());
                }
                reader.close();
            } catch (IOException ioe) {
                System.err.println("Error reading stopwords: " + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

            String[] tokens = line.split("[^\\w']+");

            Set<String> uniqueWords = new HashSet<>();

            for (String token : tokens) {
                if (token.isEmpty()) continue;

                // stopword filtering
                if (stopWords.contains(token)) continue;

                // stemming
                token = stemmer.stem(token);

                uniqueWords.add(token);
            }

            for (String w : uniqueWords) {
                word.set(w);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DF Job");

        job.setJarByClass(WordCount2_2.class);

        // VERY IMPORTANT
        job.setInputFormatClass(CustomFileInputFormat.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        int i = 0;

        if (args.length > 0 && "-skippatterns".equals(args[i])) {
            job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            job.addCacheFile(new Path(args[i + 1]).toUri());
            i += 2;
        }

        // Now correctly assign input & output
        FileInputFormat.addInputPath(job, new Path(args[i]));
        FileOutputFormat.setOutputPath(job, new Path(args[i + 1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}