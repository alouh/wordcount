package recommend;

import org.apache.hadoop.mapred.JobConf;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @Author: JF Han
 * @Date: Created in 10:07 2018/4/13
 * @Desc: 按用户分组,计算所有物品出现的组合列表,得到用户对物品的评分矩阵
 */
public class Recommend {

    static final String HDFS = "hdfs://Master:9000";
    static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String...args)throws Exception{
        Map<String,String> path = new HashMap<>(16);
        path.put("data", "E:\\ftp\\recommend\\small.csv");
        path.put("Step1Input", HDFS + "/user/hdfs/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/step1");

        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");

        path.put("Step3Input1", path.get("Step1Output"));
        path.put("Step3Output1", path.get("Step1Input") + "/step3_1");
        path.put("Step3Input2", path.get("Step2Output"));
        path.put("Step3Output2", path.get("Step1Input") + "/step3_2");
        path.put("Step4Input1", path.get("Step3Output1"));
        path.put("Step4Input2", path.get("Step3Output2"));
        path.put("Step4Output", path.get("Step1Input") + "/step4");

        Step1.run(path);
        Step2.run(path);
        System.exit(0);
    }

    static JobConf config(){
        JobConf jobConf = new JobConf(Recommend.class);
        jobConf.setJobName("Recommend");
        jobConf.addResource("classpath://core.site.xml");

        return jobConf;
    }
}
