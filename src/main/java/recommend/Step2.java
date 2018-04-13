package recommend;

import command.HdfsCommand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author: JF Han
 * @Date: Created in 11:21 2018/4/13
 * @Desc: 对物品组合列表进行技术,建立物品的同现矩阵
 */
class Step2 {
    public static class Step2UserVectorToCooccurrenceMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
        private final static Text K = new Text();
        private final static IntWritable V = new IntWritable(1);

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException{
            String[] tokens = Recommend.DELIMITER.split(text.toString());
            for (int i = 1; i < tokens.length; i++) {
                String itemID = tokens[i].split(":")[0];
                for (int j = 1; j < tokens.length; j++) {
                    String itemID2 = tokens[j].split(":")[0];
                    K.set(itemID + ":" + itemID2);
                    outputCollector.collect(K, V);
                }
            }
        }
    }
    public static class Step2UserVectorToConoccurrenceReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{
     private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException{
            int sum = 0;
            while (iterator.hasNext()) {
                sum += iterator.next().get();
            }
            result.set(sum);
            outputCollector.collect(text, result);
        }
    }

    static void run(Map<String, String> path) throws IOException{
        JobConf conf = Recommend.config();

        String input = path.get("Step2Input");
        String output = path.get("Step2Output");

        HdfsCommand hdfs = new HdfsCommand(Recommend.HDFS,conf);
        hdfs.rmr(output);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Step2UserVectorToCooccurrenceMapper.class);
        conf.setCombinerClass(Step2UserVectorToConoccurrenceReducer.class);
        conf.setReducerClass(Step2UserVectorToConoccurrenceReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }
}
