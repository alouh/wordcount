package recommend;

import command.HdfsCommand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

/**
 * @Author: JF Han
 * @Date: Created in 16:35 2018/4/17
 * @Desc:
 */
class Step4 {

    static void run(Map<String, String> path) throws IOException {
        JobConf conf = Recommend.config();

        String input1 = path.get("Step4Input1");
        String input2 = path.get("Step4Input2");
        String output = path.get("Step4Output");

        HdfsCommand hdfs = new HdfsCommand(Recommend.HDFS, conf);
        hdfs.rmr(output);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Step4PartialMultiplyMapper.class);
        conf.setCombinerClass(Step4AggregateAndRecommendReducer.class);
        conf.setReducerClass(Step4AggregateAndRecommendReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }

    public static class Step4PartialMultiplyMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable K = new IntWritable();
        private final static Text V = new Text();

        private final static Map<Integer, List<Cooccurrence>> COO_MATRIX_MAP = new HashMap<>();

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(text.toString());

            String[] v1 = tokens[0].split(":");
            String[] v2 = tokens[1].split(":");

            if (v1.length > 1) {
                int itemId1 = Integer.parseInt(v1[0]);
                int itemId2 = Integer.parseInt(v1[1]);
                int num = Integer.parseInt(tokens[1]);

                List<Cooccurrence> list;
                if (!COO_MATRIX_MAP.containsKey(itemId1)) {
                    list = new ArrayList<>();
                } else {
                    list = COO_MATRIX_MAP.get(itemId1);
                }
                list.add(new Cooccurrence(itemId1, itemId2, num));
                COO_MATRIX_MAP.put(itemId1, list);
            }

            if (v2.length > 1) {
                int itemId = Integer.parseInt(tokens[0]);
                int userId = Integer.parseInt(v2[0]);
                double pref = Double.parseDouble(v2[1]);

                K.set(userId);
                for (Cooccurrence cooccurrence : COO_MATRIX_MAP.get(itemId)) {
                    V.set(cooccurrence.getItemId2() + "," + pref * cooccurrence.getNum());
                    outputCollector.collect(K, V);
                }
            }
        }
    }

    public static class Step4AggregateAndRecommendReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        private final static Text V = new Text();

        @Override
        public void reduce(IntWritable intWritable, Iterator<Text> iterator, OutputCollector<IntWritable, Text> outputCollector, Reporter reporter) throws IOException {
            Map<String, Double> result = new HashMap<>(16);
            while (iterator.hasNext()) {
                String[] str = iterator.next().toString().split(",");
                if (result.containsKey(str[0])) {
                    result.put(str[0], result.get(str[0]) + Double.parseDouble(str[1]));
                } else {
                    result.put(str[0], Double.parseDouble(str[1]));
                }
            }
            for (Object o : result.keySet()) {
                String itemId = o.toString();
                double score = result.get(itemId);
                V.set(itemId + "," + score);
                outputCollector.collect(intWritable, V);
            }
        }
    }
}

class Cooccurrence {
    private int itemId1;
    private int itemId2;
    private int num;

    public Cooccurrence(int itemId1, int itemId2, int num) {
        super();
        this.itemId1 = itemId1;
        this.itemId2 = itemId2;
        this.num = num;
    }

    public int getItemId1() {
        return itemId1;
    }

    public void setItemId1(int itemId1) {
        this.itemId1 = itemId1;
    }

    public int getItemId2() {
        return itemId2;
    }

    public void setItemId2(int itemId2) {
        this.itemId2 = itemId2;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}