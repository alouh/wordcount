import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;  
/*reduce方法得到从map方法提交的键值对，对具有相同键的进行合并，输出。

        1.定义一个Reducer类继承自Reducer，Reducer包含有四个参数，<Text,LongWritable,Text,LongWritable>，
        Reducer抽象类的四个形式参数类型指定了reduce函数的输入和输出类型。
        在本例子中，输入键是单词，输入值是单词出现的次数，经过reduce()方法处理将单词出现的次数进行叠加，
        输出单词和单词总数。
        2.实现虚类Reducer的reduce()方法
        reduce()方法的说明：
        参数三个：
        Text                    key     -----单词
        Iterable<LongWritable>  values  -----单词出现的次数
        Context                 context -----任务的上下文，包含整个任务的全部信息

        reduce()方法的功能是：
        汇总并输出单词出现的总次数。
        由上面我们可以得知reduce()方法接收的参数：key值为单词，value值是迭代器，该迭代内存储的是单词出现的次数，
        context负责将生成的k/v输出。通过遍历values，调用values的get()方法获取Long值即为出现的次数，
        累加后context对象调用其write（）方法将结果输出。*/
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    // key: hello ,  values : {1,1,1,1,1.....}  
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values,Context context)
            throws IOException, InterruptedException {

        //定义一个累加计数器 定义为Long类型  
        long count = 0;
        for(LongWritable value:values){
            //调用value的get（）方法将long值取出来  
            count += value.get();
        }
        //输出<单词：count>键值对  
        context.write(key, new LongWritable(count));
    }
}