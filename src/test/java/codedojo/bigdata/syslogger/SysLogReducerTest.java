package codedojo.bigdata.syslogger;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Test;


public class SysLogReducerTest {
    @Test public void returnsMaximumIntegerInValues() throws IOException,
            InterruptedException {

        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new SysLogReducer())
                .withInputKey(new Text("NetworkManager: Aug 30 11"))
                .withInputValues(Arrays.asList(new IntWritable(1), new IntWritable(3)))
                .withOutput(new Text("NetworkManager: Aug 30 11"), new IntWritable(4))
                .runTest();
    }

}
