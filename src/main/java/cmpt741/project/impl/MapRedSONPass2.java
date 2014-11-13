package cmpt741.project.impl;

import static cmpt741.project.common.Params.*;

import cmpt741.project.common.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import com.google.common.collect.Sets;

/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class MapRedSONPass2 {

    public static class Pass2Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static Map<List<Integer>, Integer> itemsets = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //Read the data of the reduce step of pass1
            System.out.println("In SETUP");
            String pass1OpPath = context.getConfiguration().get(PASS1_OP.toString());
            RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
                    FileSystem.get(context.getConfiguration()).listFiles(new Path(pass1OpPath), true);
            while(locatedFileStatusRemoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();
                String path = fileStatus.getPath().toString();
                if (path.contains("part-r-")) {
                    readFromFile(path, context);
                }
            }
            System.out.println("SETUP DONE");
            System.out.println(itemsets.size());
        }

        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            // For each line in the subset
            // create all possible subsetts and check if the subset
            // is present in the data read in setup and update the counts
            System.out.println("Pass2Map");
            String[] lines = value.toString().split("\\n");
            for (String line : lines) {
                //String[] lineSplit = line.split("\\s+");
                List<Integer> lineSplit = Utils.getIntegerArray(line);
                Set lineSet = new HashSet();
                lineSet.addAll(lineSplit);
                Set powerSet = Sets.powerSet(lineSet);
                System.out.println("POWERSETS of - " + line);

                for(Object next : powerSet) {
                    List<Integer> itemSet = Utils.getIntegerArray((Set) next);
                    if (itemsets.containsKey(itemSet)) {
                        int count = itemsets.get(itemSet);
                        itemsets.put(itemSet, count+1);
                    }
                }

            }

            for (Map.Entry<List<Integer>, Integer> entrySet : itemsets.entrySet()) {
                System.out.println("WRITING: " + entrySet.getKey().toString() + " | LOCAL SUPPORT: " + entrySet.getValue().intValue());
                context.write(new Text(Utils.intArrayToString(entrySet.getKey())), new IntWritable(entrySet.getValue().intValue()));
            }
        }

        private static void readFromFile(String path, Context context) throws IOException {
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(FileSystem.get(context.getConfiguration()).open(new Path(path))));

            String line = null;
            while((line = bufferedReader.readLine()) != null) {
                List<Integer> itemSet = Utils.getIntegerArray(line);
                itemsets.put(itemSet, 0);
            }

            bufferedReader.close();
        }

    }

    public static class Pass2Red extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int minSupport = context.getConfiguration().getInt(MINIMUM_SUPPORT.toString(), 0);
            System.out.println("MINIMUM SUPPORT: " + String.valueOf(minSupport));
            int support = 0;
            for (IntWritable value : values) {
                support += value.get();
            }
            if (support >= minSupport) {
                System.out.println("WRITING - " + key.toString() + "; Support - " + String.valueOf(support));
                context.write(key, new IntWritable(support));
            } else {
                System.out.println("LEAVING - " + key.toString() + "; Support - " + String.valueOf(support));
            }
        }
    }
}
