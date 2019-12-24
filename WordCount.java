import java.io.IOException;
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

  public static class TokenizerMapper extends
  Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      String[] record=value.toString().split(",");
      Text word =new Text();
      word.set(record[10]);
      String product=record[1];
      Integer a = Integer.parseInt(product);
      int m=a.intValue();
      IntWritable i = new IntWritable();
      //if (record[7].equals("2")) {
        i.set(m);
        context.write(word,i);
      //}
    }
  }

  public static class IntSumReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Map dict=new LinkedHashMap();
      for (IntWritable val : values) {
        if(dict.containsKey(val.get())){
          int a= (int) dict.get(val.get())+1;   dict.put(val.get(),a);
        }
        else{
          int b=1;
          dict.put(val.get(),b);
        }
      }
      Map result = sortDescend(dict);
      Set<Integer> keys = result.keySet();
      IntWritable id = new IntWritable();
      for(Integer key1:keys){
        int x=key1.intValue();
        id.set(x);
        context.write(key,id);
        sum=sum+1;
        if (sum>=10){
          break;
        }
      }
    }
  }

  public static <K, V extends Comparable<? super V>> Map<K, V> sortDescend(Map<K, V> map) {

    List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());

    Collections.sort(list, new Comparator<Map.Entry<K, V>>() {

      @Override

      public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {

        int compare = (o1.getValue()).compareTo(o2.getValue());

        return -compare;

      }

    });

    Map<K, V> returnMap = new LinkedHashMap<K, V>();

    for (Map.Entry<K, V> entry : list) {

      returnMap.put(entry.getKey(), entry.getValue());

    }

    return returnMap;

  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs =
        new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
