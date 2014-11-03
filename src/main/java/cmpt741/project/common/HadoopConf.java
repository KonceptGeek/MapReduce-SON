package cmpt741.project.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class HadoopConf {

    public static Job generateConf(Class mainClass, Class mapperClass, Class reducerClass,
                                       String jobName, Class outputKeyClass,
                                       Class outputValueClass) throws IOException{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);

        job.setJarByClass(mainClass);
        job.setMapperClass(mapperClass);
        job.setCombinerClass(reducerClass);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);

        return job;
    }
}
