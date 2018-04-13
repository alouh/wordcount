package recommend;

import command.HdfsCommand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author: JF Han
 * @Date: Created in 10:18 2018/4/13
 * @Desc:
 */
class Step1 {

    public static class Step1ToItemPreMapper extends MapReduceBase implements Mapper<Object,Text,IntWritable,Text>{
       private final static IntWritable K = new IntWritable();
       private final static Text V = new Text();

        @Override
        public void map(Object o, Text text, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException{
            String[] tokens = Recommend.DELIMITER.split(text.toString());
            int userId = Integer.parseInt(tokens[0]);
            String itemId = tokens[1];
            String pref = tokens[2];
            K.set(userId);
            V.set(itemId + ":" + pref);
            outputCollector.collect(K,V);
        }
    }

    public static class Step1ToUserVectorReducer extends MapReduceBase implements Reducer<IntWritable,Text,IntWritable,Text>{
        private final static Text V = new Text();

        @Override
        public void reduce(IntWritable intWritable, Iterator<Text> iterator, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException{
            StringBuilder sb = new StringBuilder();
            while (iterator.hasNext()){
                sb.append(",").append(iterator.next());
            }
            V.set(sb.toString().replaceFirst(",",""));
            outputCollector.collect(intWritable,V);
        }
    }

    static void run(Map<String, String> path) throws IOException{
        JobConf conf = Recommend.config();

        String input = path.get("Step1Input");
        String output = path.get("Step1Output");

        HdfsCommand hdfs = new HdfsCommand(Recommend.HDFS,conf);
        hdfs.rmr(input);
        hdfs.mkdirs(input);
        hdfs.copyFile(path.get("data"),input);

        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Step1ToItemPreMapper.class);
        conf.setCombinerClass(Step1ToUserVectorReducer.class);
        conf.setReducerClass(Step1ToUserVectorReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf,new Path(input));
        FileOutputFormat.setOutputPath(conf,new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()){
            job.waitForCompletion();
        }
    }
}
