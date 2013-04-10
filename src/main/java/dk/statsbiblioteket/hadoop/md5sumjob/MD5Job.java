package dk.statsbiblioteket.hadoop.md5sumjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MD5Job extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner
        Configuration configuration = getConf();

        // Create a JobConf using the processed conf
        //JobConf jobConf = new JobConf(configuration, MD5Job.class);

        // Process custom command-line options
        //Path in = new Path(args[1]);
        //Path out = new Path(args[2]);

        // Specify job-specific parameters
        Job job = new Job(configuration, "MD5Job");
        job.setJarByClass(MD5Job.class);

        //int n = args.length;
        //if (n > 0)
        //    TextInputFormat.addInputPath(job, new Path(args[0]));
        //if (n > 1)
        //    SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MD5Map.class);
        job.setCombinerClass(MD5Reduce.class);
        job.setReducerClass(MD5Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Submit the job, then poll for progress until the job is complete
        //JobClient.runJob(jobConf);
        //return 0;

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        //int res = ToolRunner.run(new Configuration(), new MD5Job(), args);
        //System.exit(res);

        System.exit(ToolRunner.run(new MD5Job(), args));
    }
}
