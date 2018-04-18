package recommend;

import command.HdfsCommand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Map;

/**
 * @Author: JF Han
 * @Date: Created in 16:02 2018/4/17
 * @Desc:
 */
class Step3 {

    static void run1(Map<String, String> path) throws IOException {
        JobConf conf = Recommend.config();

        String input = path.get("Step3Input1");
        String output = path.get("Step3Output1");

        HdfsCommand hdfs = new HdfsCommand(Recommend.HDFS, conf);
        hdfs.rmr(output);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Step3UserVectorSplitterMapper.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }

    static void run2(Map<String, String> path) throws IOException {
        JobConf conf = Recommend.config();

        String input = path.get("Step3Input2");
        String output = path.get("Step3Output2");

        HdfsCommand hdfs = new HdfsCommand(Recommend.HDFS, conf);
        hdfs.rmr(output);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Step3CooccurrenceColumnWrapperMapper.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }

    public static class Step3UserVectorSplitterMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable K = new IntWritable();
        private final static Text V = new Text();

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(text.toString());
            for (int i = 1; i < tokens.length; i++) {
                String[] vector = tokens[i].split(":");
                int itemId = Integer.parseInt(vector[0]);
                String pref = vector[1];

                K.set(itemId);
                V.set(tokens[0] + ":" + pref);
                outputCollector.collect(K, V);
            }
        }
    }

    public static class Step3CooccurrenceColumnWrapperMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text K = new Text();
        private final static IntWritable V = new IntWritable();

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(text.toString());
            K.set(tokens[0]);
            V.set(Integer.parseInt(tokens[1]));
            outputCollector.collect(K, V);
        }
    }
}
