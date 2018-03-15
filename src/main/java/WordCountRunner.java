import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 用来描述一个作业job（使用哪个mapper类，哪个reducer类，输入文件在哪，输出结果放哪。。。。） 
 * 然后提交这个job给hadoop集群 
 * @author duanhaitao@itcast.cn 
 *两个jar包，两个类型，两个类，两个路径 
 */
//cn.itheima.bigdata.hadoop.mr.wordcount.WordCountRunner  
public class WordCountRunner {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //创建job对象需要conf对象，conf对象包含的信息是：所用的jar包，  
        Job wcjob = Job.getInstance(conf);
        //设置job所使用的jar包,使用Configuration对象调用set()方法，设置mapreduce.job.jar wcount.jar  
        conf.set("mapreduce.job.jar", "wcount.jar");

        //设置wcjob中的资源所在的jar包    
        //调用job对象的setJarByClass()方法，参数是WordCountRunner.class,设置job作业中的资源所在的jar包  
        wcjob.setJarByClass(WordCountRunner.class);

        //wcjob要使用哪个mapper类，job对象调用setMapperClass()方法，参数是WordCountMapper.class  
        wcjob.setMapperClass(WordCountMapper.class);
        //wcjob要使用哪个reducer类,job对象调用setReducerClass()方法，参数为WordCountReducer.class  
        wcjob.setReducerClass(WordCountReducer.class);

        //wcjob的mapper类输出的kv数据类型  
        //job对象调用setMapperOutputKeyClass();设置Mapper类输出的key值的类型--Text  
        //job对象调用setMapperOutputValueClass();设置Mapper类输出value值的类型--LongWritable  
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(LongWritable.class);

        //wcjob的reducer类输出的kv数据类型  
        //job对象调用setOutputKey  e
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(LongWritable.class);

        //指定要处理的原始数据所存放的路径  
        //调用FileInputFormat对象的setInputPath()方法，参数的文件路径，是设置的源数据路径，当此处为集群的路径是就是跑在集群上的程序，  
        //如果设置在当前机器的路径，就是本地模式  
        FileInputFormat.setInputPaths(wcjob, "hdfs://Master:9000/user/test1.txt");

        //指定处理之后的结果输出到哪个路径，注意此时应当在路径应当是差不多的  
        FileOutputFormat.setOutputPath(wcjob, new Path("hdfs://Master:9000/output"));
        //调用job对象的waitForCompletion()方法，提交作业。             
        boolean res = wcjob.waitForCompletion(true);
        System.exit(res?0:1);
    }
}