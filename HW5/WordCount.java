import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCount {

  public static class WordCountMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    List<String> stopwords = Arrays.asList("the", "and", "if", "he", "she", "they", "a", "an", "you", "are", "of", "is", "or");
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        if(!stopwords.contains(word))
        {context.write(word, one);}
      }
    }
  }

  public static class WordCountReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class TopNMapper extends Mapper<Object, Text, Text, IntWritable>{

    private int n = 5;
    private TreeMap<Integer, String> word_list = new TreeMap<Integer,String>();

    public void map(Object key, Text value, Context context)
    {
      String[] line = value.toString().split("\t");
      word_list.put(Integer.valueOf(line[1]), line[0]);

      if(word_list.size()>n)
        {word_list.remove(word_list.firstKey());}
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
      for(Map.Entry<Integer, String> entry : word_list.entrySet())
      {
        context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
      }
    }
  }

  public static class TopNReducer extends Reducer<Text, IntWritable, IntWritable, Text>
  {
    private int n = 5;
    private TreeMap<Integer, String>word_list = new TreeMap<Integer,String>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
    {
      int wordcount = 0;

      for(IntWritable value: values)
        {wordcount = value.get();}

      word_list.put(wordcount, key.toString());

      if(word_list.size() > n)
        {word_list.remove(word_list.firstKey());}
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
      for(Map.Entry<Integer, String> entry : word_list.entrySet())
        {context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));}
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job wcjob = Job.getInstance(conf, "word count");
    wcjob.setJarByClass(WordCount.class);
    wcjob.setMapperClass(WordCountMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    wcjob.setReducerClass(WordCountReducer.class);
    wcjob.setMapOutputKeyClass(Text.class);
    wcjob.setMapOutputValueClass(IntWritable.class);
    wcjob.setOutputKeyClass(Text.class);
    wcjob.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(wcjob, new Path(args[0]));
    FileOutputFormat.setOutputPath(wcjob, new Path(args[1]));
    wcjob.waitForCompletion(true);

    Job topnjob = Job.getInstance(conf, "TopN");
    topnjob.setJarByClass(WordCount.class);
    topnjob.setMapperClass(TopNMapper.class);
    topnjob.setReducerClass(TopNReducer.class);
    topnjob.setMapOutputKeyClass(Text.class);
    topnjob.setMapOutputValueClass(IntWritable.class);
    topnjob.setOutputKeyClass(IntWritable.class);
    topnjob.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(topnjob, new Path(args[1]));
    FileOutputFormat.setOutputPath(topnjob,new Path(args[2]) );
    System.exit(topnjob.waitForCompletion(true) ? 0 : 1);
  }
}
