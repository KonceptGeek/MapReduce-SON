package cmpt741.project.impl;

import cmpt741.project.common.HadoopConf;
import static cmpt741.project.common.Params.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class Executor {

    public static void main(String args[]) throws Exception {
        //Create job conf for Pass1
        Job pass1Job = HadoopConf.generateConf(MapRedSONPass1.class, MapRedSONPass1.Pass1Map.class,
                MapRedSONPass1.Pass1Reduce.class, "jsabharw-MapRedSONPass1",
                Text.class, IntWritable.class);
        pass1Job.setNumReduceTasks(10);
        pass1Job.getConfiguration().setInt(MINIMUM_SUPPORT.toString(), 500);
        pass1Job.getConfiguration().set(ITEM_SPLIT.toString(), "\\s+");

        System.out.println("INPUT PATH - " + args[0]);
        System.out.println("OUTPUT PATH - " + args[1]);

        System.out.println("############# Executing Pass1 Map Reduce #############");
        FileInputFormat.setInputPaths(pass1Job, new Path(args[0]));
        FileOutputFormat.setOutputPath(pass1Job, new Path(args[1]));

        System.exit(pass1Job.waitForCompletion(true) ? 0 : 1);
    }
}
