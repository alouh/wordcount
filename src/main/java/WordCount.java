import command.HdfsCommand;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * @Author: HanJiafeng
 * @Date: 10:08 2018/3/26
 * @Desc:
 */
public class WordCount {

    public static class WordCountMapper extends MapReduceBase implements Mapper<Object,Text,Text,IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object o, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException{
            StringTokenizer stringTokenizer = new StringTokenizer(text.toString());
            while (stringTokenizer.hasMoreTokens()){
                word.set(stringTokenizer.nextToken());
                outputCollector.collect(word,one);
            }
        }
    }

    public static class WordCountReucer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException{

            int sum = 0;
            while (iterator.hasNext()){
                sum += iterator.next().get();
            }
            result.set(sum);
            outputCollector.collect(text,result);
        }
    }

    public static void main(String...args)throws Exception{

        String input = "hdfs://Master:9000/user/test1.txt";
        //String output = "hdfs://Master:9000/user/output";

        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("WordCount");
        conf.addResource("classpath://core-site.xml");
        //conf.addResource("classpath://hdfs-site.xml");
        //conf.addResource("classpath://mapred-site.xml");

        HdfsCommand hdfsCommand = new HdfsCommand(input,conf);
        hdfsCommand.cat("/output/part-r-00000");

        /*conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordCountMapper.class);
        conf.setCombinerClass(WordCountReucer.class);
        conf.setReducerClass(WordCountReucer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf,new Path(input));
        FileOutputFormat.setOutputPath(conf,new Path(output));

        WordCountRunner.deleteDir(conf,output);

        JobClient.runJob(conf);
        System.exit(0);*/
    }
}
