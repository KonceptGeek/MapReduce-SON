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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class Executor {

    public static void main(String args[]) throws Exception {
        int minSupport = Integer.parseInt(args[0]);
        String inputPath = args[1];
        int numSplits = Integer.parseInt(args[2]);
        String outputPath = args[3];
        String pass1TempPath = outputPath+"_temp";

        String splitsLocation = splitInputFile(inputPath, numSplits);
        boolean pass1Completion = setupAndStartPass1(splitsLocation, pass1TempPath, minSupport);
        if (pass1Completion) {
            setupAndStartPass2(splitsLocation, pass1TempPath, outputPath, minSupport);
        }

    }


    private static String splitInputFile(String inputPath, int numSplits) {
        return inputPath;
    }

    private static boolean setupAndStartPass1(String inputPath, String outputPath,
                                              int minSupport) throws IOException,
            ClassNotFoundException, InterruptedException {
        System.out.println("SUBMITTING PASS 1");


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

        pass1Job.getConfiguration().setLong("mapreduce.task.timeout", 1000000);



        System.out.println("INPUT PATH - " + inputPath);
        System.out.println("OUTPUT PATH - " + outputPath);

        System.out.println("############# Executing Pass1 Map Reduce #############");

        FileInputFormat.setInputPaths(pass1Job, new Path(inputPath));
        FileOutputFormat.setOutputPath(pass1Job, new Path(outputPath));

        return pass1Job.waitForCompletion(true);
    }

    private static boolean setupAndStartPass2(String inputPath, String pass1OpPath,
                                              String outputPath, int minSupport) throws IOException,
            ClassNotFoundException, InterruptedException {
        System.out.println("SUBMITTING PASS 2");
        Job pass2Job = HadoopConf.generateConf(MapRedSONPass2.class, MapRedSONPass2.Pass2Map.class,
                MapRedSONPass2.Pass2Red.class, "jsabharw-MapRedSONPass2", Text.class, IntWritable.class,
                Text.class, IntWritable.class, CustomFileInputFormat.class);

        pass2Job.getConfiguration().set(PASS1_OP.toString(), pass1OpPath);
        pass2Job.getConfiguration().set(ITEM_SPLIT.toString(), "\\s+");
        pass2Job.getConfiguration().setInt(MINIMUM_SUPPORT.toString(), minSupport);
            pass2Job.getConfiguration().setLong("mapreduce.task.timeout", 3000000);

        FileInputFormat.setInputPaths(pass2Job, new Path(inputPath));
        FileOutputFormat.setOutputPath(pass2Job, new Path(outputPath));

        return pass2Job.waitForCompletion(true);
    }
}
