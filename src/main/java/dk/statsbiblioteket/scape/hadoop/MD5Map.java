package dk.statsbiblioteket.scape.hadoop;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: bam
 * Date: 2/8/13
 * Time: 10:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class MD5Map implements Mapper {
    @Override
    public void map(Object o, Object o2, OutputCollector outputCollector, Reporter reporter) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void configure(JobConf entries) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
