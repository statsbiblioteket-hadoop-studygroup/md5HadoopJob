package dk.statsbiblioteket.hadoop.md5sumjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * https://github.com/statsbiblioteket-hadoop-studygroup
 * User: bam
 * Date: 2/8/13
 */
public class MD5Map extends Mapper<LongWritable, Text, Text, LongWritable> {
    final private static LongWritable ONE = new LongWritable(1);

    @Override
    protected void map(LongWritable line, Text text, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {

        ProcessBuilder pb = new ProcessBuilder("md5sum", text.toString());
        //set the working directory to a temporary directory
        pb.directory(new File("/tmp/hadoopmd5/"));

        //pb.environment().put("PATH", "/usr/bin/");

        //start the executable
        Process proc = pb.start();
        //create a log of the console output??
        BufferedReader stdout = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        BufferedReader stderr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

        context.write(stdout, ONE);//TODO

        try {
            //wait for process to end before continuing
            proc.waitFor();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
