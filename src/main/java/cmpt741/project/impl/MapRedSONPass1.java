package cmpt741.project.impl;

import cmpt741.project.common.Utils;
import cmpt741.project.models.Transaction;
import static cmpt741.project.common.Params.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class MapRedSONPass1 {

    public static class Pass1Map extends Mapper<LongWritable, Text,
            Text, IntWritable> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            String splitRegex = context.getConfiguration().get(ITEM_SPLIT.toString());
            if (splitRegex == null) {
                splitRegex = "\\s+";
                System.err.println("Split regex not found");
            }

            Path path = ((FileSplit) context.getInputSplit()).getPath();
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            System.out.println("Filepath: " + path.toString());
            if (fileSystem == null) {
                System.out.println("Filesystem is null");
            }

            List<Transaction> transactions = Utils.readTransactionsFromHadoop(path, splitRegex, fileSystem);

            System.out.println("Number of transactions: " + String.valueOf(transactions.size()));
        }

        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numMappers = Integer.parseInt(conf.get("mapred.map.tasks"));


            context.write(value, new IntWritable(1));
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
