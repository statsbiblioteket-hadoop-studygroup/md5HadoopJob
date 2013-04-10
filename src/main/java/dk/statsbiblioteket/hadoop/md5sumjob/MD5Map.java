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

public class MD5Map extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable offset, Text text, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {

        Runtime rt = Runtime.getRuntime();

        Process pr = rt.exec("md5sum");

        BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

        String line=null;

        while((line=input.readLine()) != null) {
            System.out.println(line);
        }

        int exitVal = 0;
        try {
            exitVal = pr.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Exited with error code "+exitVal);
    }

}
