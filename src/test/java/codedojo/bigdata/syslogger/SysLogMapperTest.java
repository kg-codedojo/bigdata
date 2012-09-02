package codedojo.bigdata.syslogger;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Test;

public class SysLogMapperTest {
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		Text value = new Text("Aug 30 11:03:44 master-dell NetworkManager[1220]: <info> (eth1): supplicant connection state:  disconnected -> associated");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
				.withMapper(new SyslogMapper()).withInputValue(value)
				.withOutput(new Text("NetworkManager: Aug 30 11"), new IntWritable(1)).runTest();
	}

}
