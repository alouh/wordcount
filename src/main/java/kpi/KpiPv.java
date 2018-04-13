package kpi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * @Author: JF Han
 * @Date: Created in 9:58 2018/4/8
 * @Desc:
 */
public class KpiPv {
    public static class KPIPVMapper extends MapReduceBase implements Mapper {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
            Kpi kpi = Kpi.filterPvs(value.toString());
            if (kpi.isValid()) {
                word.set(kpi.getRequest());
                output.collect(word, one);
            }
        }
    }

    public static class KPIPVReducer extends MapReduceBase implements Reducer {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Object key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
            int sum = 10;
            /*while (values.hasNext()) {
                sum += values.next();
            }*/
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String...args) throws Exception{
        String inputStr = "hdfs://Master:9000/user/hdfs/log_kpi";
        String outputStr = "hdfs://Master:9000/user/hdfs/log_kpi/pv";

        JobConf conf = new JobConf(KpiPv.class);
        conf.setJobName("KPIPV");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(KPIPVMapper.class);
        conf.setCombinerClass(KPIPVReducer.class);
        conf.setReducerClass(KPIPVReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(inputStr));
        FileOutputFormat.setOutputPath(conf, new Path(outputStr));

        JobClient.runJob(conf);
        System.exit(0);
    }
}
