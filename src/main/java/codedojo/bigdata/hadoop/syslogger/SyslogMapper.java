package codedojo.bigdata.hadoop.syslogger;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SyslogMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable rowNum, Text row,
			OutputCollector<Text, IntWritable> collector, Reporter reporter)
			throws IOException {
		
		//Aug 30 11:03:44 master-dell NetworkManager[1220]: <info>
		Pattern pattern = Pattern.compile("(.{9,9}).*master-dell (\\w*).*");
		
		String value = row.toString();
		
		Matcher mathcer = pattern.matcher(value);
	    
		String key = "";
		if(mathcer.matches()){
			key = mathcer.group(2)+": "+mathcer.group(1);
		}
		
		collector.collect(new Text(key), new IntWritable(1));
		
	}

}
