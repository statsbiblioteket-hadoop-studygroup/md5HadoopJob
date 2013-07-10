package dk.statsbiblioteket.hadoop.md5sumjob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * eu.scape_project
 * User: bam
 * Date: 7/4/13
 * Time: 10:57 AM
 */
public class MD5JobTest {
    MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
    MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

    Text inputFilePath = new Text("/home/bam/Projects/md5mapreduce/src/test/resources/thermo.wav");
    Text output = new Text("/usr/bin/md5sum\n" +
            "a9ecebfc7077312db5b483c0cae6dd73  /home/bam/Projects/md5mapreduce/src/test/resources/thermo.wav\n" +
            "\n");

    @Before
    public void setUp() {
        ProcessBuilder pb = new ProcessBuilder("/usr/bin/md5sum", "/home/bam/Projects/md5mapreduce/src/test/resources/thermo.wav");
        try {
            Process proc = pb.start();
            System.out.println((new BufferedReader(new InputStreamReader(proc.getInputStream()))).readLine());
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        MD5Map mapper = new MD5Map();
        MD5Reduce reducer = new MD5Reduce();
        mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new LongWritable(0), inputFilePath);
        mapDriver.withOutput(output, new LongWritable(0));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(0));
        reduceDriver.withInput(output, values);
        reduceDriver.withOutput(output, new LongWritable(0));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() {
        mapReduceDriver.withInput(new LongWritable(1), inputFilePath);
        mapReduceDriver.addOutput(output, new LongWritable(0));
        mapReduceDriver.runTest();
    }

}
