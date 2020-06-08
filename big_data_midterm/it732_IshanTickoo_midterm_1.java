import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.Random;
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



public class MidTermQ1 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();




        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
//            StringTokenizer itr = new StringTokenizer(value.toString());
            Integer k_value = new Integer(context.getConfiguration().get("k_value"));
            String lines[] = value.toString().split("\\r?\\n");
            for(String line : lines){
                Random rand = new Random();
                String cur_ind = String.valueOf(rand.nextInt(k_value));
                String double_val = String.valueOf(Math.random());
                String output = double_val + "~" + line;
                IntWritable op = new IntWritable(Integer.valueOf(cur_ind));
                context.write(op, new Text(output));
            }
        }
    }



    public static class IntSumReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {
//        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            double max_val = Double.MIN_VALUE;
            String finalKey = "";
            for(Text cur: values) {
                String cur_string = cur.toString();
                Boolean isComma = false;
                StringBuilder value1 = new StringBuilder();
                StringBuilder value2 = new StringBuilder();
                for(int i = 0; i < cur_string.length(); i++){
                    char ch = cur_string.charAt(i);
                    if(ch == '~'){
                        isComma = true;
                    }
                    else if(!isComma){
                        value1.append(ch);
                    }
                    else if(Double.valueOf(value1.toString()) < max_val){
                        break;
                    }
                    else {
                        value2.append(ch);
                    }
                }
                if(value1.length() == 0) continue;
                Boolean isNumeric = true;
                for(int i = 0; i < value1.length(); i++){
                    char ch = value1.charAt(i);
                    if(ch != '.' && !Character.isDigit(ch)) {
                        isNumeric = false;
                        break;
                    }
                }
                if(!isNumeric) {
//                    context.write(key, new Text(value1.toString()));
                    continue;
                }
                Double random_val = Double.valueOf(value1.toString());
                if(random_val > max_val) {
                    max_val = random_val;
                    finalKey = value2.toString();
                }
            }
            context.write(key, new Text(finalKey));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("k_value", args[2]);
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MidTermQ1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
