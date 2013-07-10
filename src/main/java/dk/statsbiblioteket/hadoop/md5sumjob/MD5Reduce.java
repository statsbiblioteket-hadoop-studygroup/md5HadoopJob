package dk.statsbiblioteket.hadoop.md5sumjob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * https://github.com/statsbiblioteket-hadoop-studygroup
 * User: bam
 * Date: 2/8/13
 */
public class MD5Reduce extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable total = new LongWritable();

    @Override
    protected void reduce(Text logPath, Iterable<LongWritable> exitCodes, Context context)
            throws IOException, InterruptedException {//TODO
        long n = 0;
        for (LongWritable count : exitCodes)
            n += count.get();
        total.set(n);
        context.write(logPath, total);
    }
}
