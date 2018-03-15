import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;  
/**
 我们自定义的Mapper类继承自Mapper（extends）实现其map()方法。
 Mapper类含有四个参数分别代表着输入和输出，可以结合hadoop的内置封装的数据类型来理解。

 map()的功能：
 接收键值对，输出中间键值对，是将整个任务分解为多个小任务。其后由MapReduce框架将键值相同的值传给reduce方法。
 map()方法中的参数说明：
 共有三个参数：LongWritable key     ---键值对中key
 Text         value   ---键值对中的value
 Context      context ---记录输入的key/value,记录key/value的运算状态
 这里扩展的内容是hadoop内置的数据类型：
 BooleanWriteable :标准布尔型数值
 ByteWritable :单字节数值
 DoubleWritable: 双字节数值
 FloatWritable :浮点数
 IntWritable : 整型数
 LongWritable: 长整型数
 Text        ：使用UTF-8格式存储的文本
 NullWritable：当<key,value>中的key或value为空时使用

 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

    /**
     map（）方法实现思路：
     1.获取文件的每行内容
     2.将这行内容切分，调用StringUtils的方法是split方法，分割符为“”，切分后的字符放到字符串数组内
     3.遍历输出<word,1>
     */
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {

        //获取到一行文件的内容  
        String line = value.toString();
        //切分这一行的内容为一个单词数组  
        String[] words = StringUtils.split(line, " ");
        //遍历  输出  <word,1>  
        for(String word:words){

            context.write(new Text(word), new LongWritable(1));

        }
    }
}  