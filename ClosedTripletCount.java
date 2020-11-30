import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class ClosedTripletCount extends Configured implements Tool {
    public static class FirstMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            String[] pair = text.toString().split("\\s+");
            if (pair.length > 1) { // if edge is valid
                long u = Long.parseLong(pair[0]);
                long v = Long.parseLong(pair[1]);

                context.write(new LongWritable(u), new LongWritable(v));
                context.write(new LongWritable(v), new LongWritable(u));

                // if (u < v) {
                //     context.write(new LongWritable(u), new LongWritable(v));
                //     context.write(new LongWritable(v), new LongWritable(u));
                // } else {
                //     context.write(new LongWritable(v), new LongWritable(u));
                //     context.write(new LongWritable(u), new LongWritable(v));
                // }
            }
        }
    }

    public static class FirstReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<Long> valuesCopy = new ArrayList<Long>();
            for (LongWritable u : values) {
                valuesCopy.add(u.get());
                context.write(new Text(key.toString() + ',' + u.toString()), new Text("$"));
            }
            for (int u = 0; u < valuesCopy.size(); ++u) {
                for (int w = u; w < valuesCopy.size(); ++w) {
                    int compare = valuesCopy.get(u).compareTo(valuesCopy.get(w));
                    if (compare < 0) {
                        context.write(new Text(valuesCopy.get(u).toString() + ',' + valuesCopy.get(w).toString()), new Text(key.toString()));
                    }
                }
            }
        }
    }

    public static class SecondMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            String[] pair = text.toString().split("\\s+");
            if (pair.length > 1) {
                context.write(new Text(pair[0]), new Text(pair[1]));
            }
        }
    }

    public static class SecondReducer extends Reducer<Text, Text, LongWritable, LongWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            LinkedHashSet<String> valueSet = new LinkedHashSet<String>();
            for (Text value: values) {
                valueSet.add(value.toString());
            }
            long count = 0;
            boolean valid = false;
            for (String value: valueSet) {
                if (!value.equals("$")) {
                    ++count;
                } else {
                    valid = true;
                }
            }
            if (valid) {
                context.write(new LongWritable(0), new LongWritable(count));
            }
        }
    }

    public static class ThirdMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            String[] pair = text.toString().split("\\s+");
            if (pair.length > 1) {
                context.write(new LongWritable(0), new LongWritable(Long.parseLong(pair[1])));
            }
        }
    }

    public static class ThirdReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable> {
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(new Text("Result"), new LongWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        Job jobOne = new Job(getConf());
        jobOne.setJobName("first-mapreduce");

        jobOne.setJar("triplet.jar");

        jobOne.setMapOutputKeyClass(LongWritable.class);
        jobOne.setMapOutputValueClass(LongWritable.class);

        jobOne.setOutputKeyClass(Text.class);
        jobOne.setOutputValueClass(Text.class);

        jobOne.setJarByClass(ClosedTripletCount.class);
        jobOne.setMapperClass(FirstMapper.class);
        jobOne.setReducerClass(FirstReducer.class);

        FileInputFormat.addInputPath(jobOne, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobOne, new Path("./temp/first-mapreduce"));

        Job jobTwo = new Job(getConf());
        jobTwo.setJobName("second-mapreduce");

        jobTwo.setJar("triplet.jar");

        jobTwo.setMapOutputKeyClass(Text.class);
        jobTwo.setMapOutputValueClass(Text.class);

        jobTwo.setOutputKeyClass(LongWritable.class);
        jobTwo.setOutputValueClass(LongWritable.class);

        jobTwo.setJarByClass(ClosedTripletCount.class);
        jobTwo.setMapperClass(SecondMapper.class);
        jobTwo.setReducerClass(SecondReducer.class);

        FileInputFormat.addInputPath(jobTwo, new Path("./temp/first-mapreduce"));
        FileOutputFormat.setOutputPath(jobTwo, new Path("./temp/second-mapreduce"));

        Job jobThree = new Job(getConf());
        jobThree.setJobName("third-mapreduce");
        jobThree.setNumReduceTasks(1);

        jobThree.setJar("triplet.jar");

        jobThree.setMapOutputKeyClass(LongWritable.class);
        jobThree.setMapOutputValueClass(LongWritable.class);

        jobThree.setOutputKeyClass(Text.class);
        jobThree.setOutputValueClass(LongWritable.class);

        jobThree.setJarByClass(ClosedTripletCount.class);
        jobThree.setMapperClass(ThirdMapper.class);
        jobThree.setReducerClass(ThirdReducer.class);

        FileInputFormat.addInputPath(jobThree, new Path("./temp/second-mapreduce"));
        FileOutputFormat.setOutputPath(jobThree, new Path(args[1]));

        int ret = jobOne.waitForCompletion(true) ? 0 : 1;
        if (ret == 0)
            ret = jobTwo.waitForCompletion(true) ? 0 : 1;
        if (ret == 0)
            ret = jobThree.waitForCompletion(true) ? 0 : 1;

        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ClosedTripletCount(), args);
        System.exit(res);
    }
}