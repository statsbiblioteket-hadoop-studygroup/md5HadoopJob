package dk.statsbiblioteket.hadoop.md5sumjob;

import eu.scape_project.tb.tavernahadoopwrapper.Tools;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

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

    @Override
    protected void map(LongWritable key, Text inputFilePath, Context context) throws IOException, InterruptedException {

        List<String> commandLine = new ArrayList<String>();
        commandLine.add("/usr/bin/md5sum");
        commandLine.add(inputFilePath.toString());
        ProcessBuilder pb = new ProcessBuilder("/usr/bin/md5sum", inputFilePath.toString());
        //set the working directory to a temporary directory
        //pb.directory(new File("/tmp/hadoopmd5/"));

        //pb.environment().put("PATH", "/usr/bin/");

        //start the executable
        Process proc = pb.start();
        BufferedReader stdout = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        BufferedReader stderr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        try {
            //wait for process to end before continuing
            proc.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int exitCode = proc.exitValue();

        /*
        //store the log file of the console output
        String logFilePath = inputFilePath.toString() + "_md5sum.log";
        Path outputPath = SequenceFileOutputFormat.getOutputPath(context);
        if (outputPath != null) {
            String[] inputFilePathArray = inputFilePath.toString().split("/");
            logFilePath = outputPath.toString() + "/" + inputFilePathArray[inputFilePathArray.length-1] + "_md5sum.log";
        }

        BufferedWriter outputFile = new BufferedWriter(new FileWriter(logFilePath,true));
        outputFile.write("***");
        outputFile.newLine();

        //write the command line to the file
        Tools.appendProcessInfoToLog(exitCode, commandLine, outputFile);
        //write the log of stdout and stderr to the logfile
        Tools.appendBufferToFile("stdout", stdout, outputFile);
        Tools.appendBufferToFile("stderr", stderr, outputFile);
        outputFile.close();
        */

        String stdoutString = stdout.ready() ? stdout.readLine() : "";
        String stderrString = stderr.ready() ? stderr.readLine() : "";
        Text output = new Text("/usr/bin/md5sum\n" + stdoutString + "\n" + stderrString + "\n");

        context.write(output, new LongWritable(exitCode));

    }

}
