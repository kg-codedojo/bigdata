package codedojo.bigdata.hadoop.meter;

import codedojo.bigdata.hadoop.meters.InputDataProcessor;
import codedojo.bigdata.hadoop.meters.MeterMapper;
import codedojo.bigdata.hadoop.meters.MeterReducer;
import codedojo.bigdata.hadoop.temperature.MaxTemperatureReducer;
import codedojo.bigdata.hadoop.utils.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.junit.Test;

/**
 * Created By: KonstantinG
 * Date,time: 9/9/12, 9:52 PM
 */
public class MeterJobRunnerTest {
    @Test public void testRunJob() throws Exception{
        InputDataProcessor processor = new InputDataProcessor("data/preprocessed");
        processor.preprocess("data/fdata.csv","fdata.csv","stationID1");
        /* TODO: Currently uses local file system => Should be test for HDFS also
        HDFSUtil util = new HDFSUtil("/home/master/dev/hadoop");
        util.mkdir("/data");
        util.mkdir("/data/output");
        util.mkdir("/data/output/_temporary");
        util.addFile("data/preprocessed/fdata.csv","/data");
        */
        JobConf conf = new JobConf();
        conf.setJobName("Meter");

        //setting key value types for mapper and reducer outputs
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        //specifying the custom reducer class
        conf.setMapperClass(MeterMapper.class);
        conf.setReducerClass(MeterReducer.class);

        //Specifying the input directories(@ runtime) and Mappers        independently for inputs from multiple sources
        FileInputFormat.addInputPath(conf, new Path("/home/master/codedojo/bigdata/data/preprocessed/fdata.csv"));

        //Specifying the output directory @ runtime
        FileOutputFormat.setOutputPath(conf, new Path("/home/master/codedojo/bigdata/data/output"));

        JobClient.runJob(conf);
    }
}
