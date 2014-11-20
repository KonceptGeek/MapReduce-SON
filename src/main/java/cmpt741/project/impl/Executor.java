package cmpt741.project.impl;

import cmpt741.project.hadoop.CustomFileInputFormat;
import cmpt741.project.hadoop.HadoopConf;
import static cmpt741.project.common.Params.*;

import cmpt741.project.common.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;


/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class Executor {

    public static void main(String args[]) throws Exception {
        //filePath, k, s
        String inputFile = args[0];
        int numSplits = Integer.parseInt(args[1]);
        float supportPercentage = Float.parseFloat(args[2]);
        String outputPath = args[3];
        String resultPath = args[4];

        String splitsLocation = outputPath+"_splits/";
        String pass1TempPath = outputPath+"_temp";

        int totalTransactions = splitInputFile(inputFile, splitsLocation, numSplits);
        int minSupport = (int)((supportPercentage / 100) * totalTransactions);
        System.out.println("TOTAL TRANSACTIONS- " + String.valueOf(totalTransactions));
        System.out.println("MIN-SUPPORT- " + String.valueOf(minSupport));

        boolean pass1Completion = setupAndStartPass1(splitsLocation, pass1TempPath, minSupport);
        if (pass1Completion) {
            setupAndStartPass2(splitsLocation, pass1TempPath, outputPath, minSupport);
        }
        printResults(outputPath, resultPath);
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

        //
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

        System.out.println("INPUT PATH - " + inputPath);
        System.out.println("OUTPUT PATH - " + outputPath);

        System.out.println("############# Executing Pass2 Map Reduce #############");

        FileInputFormat.setInputPaths(pass2Job, new Path(inputPath));
        FileOutputFormat.setOutputPath(pass2Job, new Path(outputPath));

        return pass2Job.waitForCompletion(true);
    }

    private static int splitInputFile(String inputFile, String splitsLocation, int numSplits) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        int totalLineCount = Utils.getLinesInHadoopFile(new Path(inputFile), fs);
        int numLinesPerSplit = (int) Math.ceil(totalLineCount/numSplits) + 1;

        BufferedReader inputReader =
                new BufferedReader(new InputStreamReader(fs.open(new Path(inputFile))));

        BufferedWriter outputWriter = null;

        String line;
        int linesWritten = 0;
        int totalLinesWritten = 0;
        int fileNumber = 1;
        while((line = inputReader.readLine()) != null) {

            if (linesWritten == 0) {
                outputWriter = new BufferedWriter(
                        new OutputStreamWriter(fs.create(
                                new Path(splitsLocation+"part_"+String.valueOf(fileNumber)))));
                fileNumber++;
            }
            outputWriter.write(line+"\n");
            linesWritten++;
            totalLinesWritten++;
            if (linesWritten == numLinesPerSplit || totalLinesWritten == totalLineCount) {
                outputWriter.close();
                linesWritten = 0;
            }
        }
        inputReader.close();

        return totalLineCount;
    }

    private static void printResults(String outputPath, String resultPath) throws Exception{
        Map<String, Integer> unsortedMap = new HashMap<>();
        ValueComparator valueComparator = new ValueComparator(unsortedMap);
        Map<String, Integer> sortedMap = new TreeMap<>(valueComparator);


        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path(outputPath), true);


        while(remoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = remoteIterator.next();
            String path = fileStatus.getPath().toString();
            if (path.contains("part-r-")) {
                BufferedReader inputReader =
                        new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while((line = inputReader.readLine()) != null) {
                    String[] lineSplit = line.split("\t");
                    unsortedMap.put(lineSplit[0], Integer.valueOf(lineSplit[1]));
                }
                inputReader.close();
            }
        }
        sortedMap.putAll(unsortedMap);

        System.out.println("\n\n\n##################### RESULTS #####################\n\n\n");

        System.out.println(unsortedMap.size());
        BufferedWriter opWriter = new BufferedWriter(new FileWriter(new File(resultPath)));
        opWriter.write(String.valueOf(unsortedMap.size()) + "\n");
        for (Map.Entry<String, Integer> entrySet : sortedMap.entrySet()) {
            System.out.println(entrySet.getKey() + "\t(" + String.valueOf(entrySet.getValue()) + ")");
            opWriter.write(entrySet.getKey() + "\t(" + String.valueOf(entrySet.getValue()) + ")\n");
        }
        opWriter.close();
    }

    static class ValueComparator implements Comparator<String> {

        Map<String, Integer> base;
        public ValueComparator(Map<String, Integer> base) {
            this.base = base;
        }

        // Note: this comparator imposes orderings that are inconsistent with equals.
        public int compare(String a, String b) {
            if (base.get(a) > base.get(b)) {
                return -1;
            } else if (base.get(a) < base.get(b)){
                return 1;
            } else {
                return a.compareTo(b);
            }
        }
    }



}
