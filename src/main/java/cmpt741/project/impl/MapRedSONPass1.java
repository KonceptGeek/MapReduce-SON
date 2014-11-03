package cmpt741.project.impl;

import cmpt741.project.models.Transaction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;


import java.io.IOException;
import java.util.Iterator;

/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class MapRedSONPass1 {

    public static class Pass1Map extends Mapper<LongWritable, Text,
            Text, IntWritable> {

        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numMappers = Integer.parseInt(conf.get("mapred.map.tasks"));
            Transaction transaction =
            Text word = new Text();
            word.set("map1");

            context.write(word, new IntWritable(1));
        }
    }

    public static class Pass1Reduce extends Reducer<Text, IntWritable,
            Text, IntWritable> {

        public void reduce(Text text, Iterator<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            context.write(text, new IntWritable(1));
        }
    }
}
