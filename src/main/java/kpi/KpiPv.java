package kpi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: JF Han
 * @Date: Created in 9:58 2018/4/8
 * @Desc:
 */
public class KpiPv {
    public static class KPIPVMapper extends MapReduceBase implements Mapper<Object,Object,Text,IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Object value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
            Kpi kpi = Kpi.filterPvs(value.toString());
            if (kpi.isValid()) {
                word.set(kpi.getRequest());
                output.collect(word, one);
            }
        }
    }

    public static class KPIPVReducer extends MapReduceBase implements Reducer<Object,Text,Object,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Object o, Iterator<Text> iterator, OutputCollector<Object, IntWritable> outputCollector, Reporter reporter) throws IOException{
            // TODO: 2018/4/18 那个网站上的内容有问题,接口后面没有跟类型,代码不全.url:http://blog.fens.me/hadoop-mapreduce-log-kpi/
            int sum = 10;
            while (iterator.hasNext()) {
                sum += iterator.next().getLength();
            }
            result.set(sum);
            outputCollector.collect(o, result);
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
