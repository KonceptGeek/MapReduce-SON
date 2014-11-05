package cmpt741.project.impl;

import cmpt741.project.hadoop.CustomFileInputFormat;
import cmpt741.project.hadoop.HadoopConf;
import static cmpt741.project.common.Params.*;

import cmpt741.project.common.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class Executor {

    public static void main(String args[]) throws Exception {
        int minSupport = Integer.parseInt(args[0]);
        String inputPath = args[1];
        String outputPath = args[2];

        //Create job conf for Pass1
        Job pass1Job = HadoopConf.generateConf(MapRedSONPass1.class,
                MapRedSONPass1.Pass1Map.class, MapRedSONPass1.Pass1Reduce.class,
                "jsabharw-MapRedSONPass1", Text.class, IntWritable.class,
                Text.class, NullWritable.class, CustomFileInputFormat.class);


        //pass1Job.setNumReduceTasks(10);
        pass1Job.getConfiguration().setInt(MINIMUM_SUPPORT.toString(), minSupport);
        pass1Job.getConfiguration().set(ITEM_SPLIT.toString(), "\\s+");

        //int totalLineCount = Utils.getLinesInHadoopFile(new Path(inputPath), FileSystem.get(pass1Job.getConfiguration()));
        pass1Job.getConfiguration().setInt(TOTAL_TRANSACTIONS.toString(), 100000);


        System.out.println("INPUT PATH - " + inputPath);
        System.out.println("OUTPUT PATH - " + outputPath);
        
        System.out.println("############# Executing Pass1 Map Reduce #############");

        FileInputFormat.setInputPaths(pass1Job, new Path(inputPath));
        FileOutputFormat.setOutputPath(pass1Job, new Path(outputPath));

        System.exit(pass1Job.waitForCompletion(true) ? 0 : 1);
    }
}
