package cmpt741.project.impl;

import static cmpt741.project.common.Params.*;

import cmpt741.project.apriori.Apriori;
import cmpt741.project.apriori.Database;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class MapRedSONPass1 {

    public static class Pass1Map extends Mapper<LongWritable, Text,
            Text, IntWritable> {

        /*@Override
        public void setup(Context context) throws IOException, InterruptedException {
            String splitRegex = context.getConfiguration().get(ITEM_SPLIT.toString());
            int totalTransactions = context.getConfiguration().getInt(TOTAL_TRANSACTIONS.toString(), 0);
            if (totalTransactions == 0) {
                System.exit(1);
            }
            System.out.println("Total Transactions - " + String.valueOf(totalTransactions));

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
        }*/

        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            //int numMappers = Integer.parseInt(conf.get("mapred.map.tasks"));
            int minSupport = conf.getInt(MINIMUM_SUPPORT.toString(), 0);
            int totalTransactions = conf.getInt(TOTAL_TRANSACTIONS.toString(), 0);

            if (minSupport == 0 || totalTransactions == 0) {
                throw new InterruptedException("Support or Total Transactions is 0");
            }

            Database db = new Database(value.toString());
            int numTransactions = db.dbSize();

            int supportForMap = (int) Math.ceil((((double) numTransactions)/((double) totalTransactions)) * minSupport);

            System.out.println("\nSupport for map: " + String.valueOf(supportForMap));
            System.out.println("\nTransactions being processed by map: " + String.valueOf(numTransactions));

            System.out.println("\nStarting Apriori");
            Apriori apriori = new Apriori("Map1-Apriori", db, supportForMap);
            Apriori.debugger = true;
            apriori.start();
            try {
                apriori.join();
                apriori.printPatterns();
            } catch (Exception e) {
                e.printStackTrace();
            }
            List<List<Integer>> frequentItemsets = apriori.getFrequentItemsets();
            for (List<Integer> itemset : frequentItemsets) {
                String output = "";
                Collections.sort(itemset);
                for (Integer item : itemset) {
                    output += String.valueOf(item) + " ";
                }
                context.write(new Text(output.trim()), new IntWritable(1));
            }
        }
    }

    public static class Pass1Reduce extends Reducer<Text, IntWritable,
            Text, NullWritable> {

        public void reduce(Text text, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            context.write(new Text(text.toString()), NullWritable.get());
        }
    }
}
