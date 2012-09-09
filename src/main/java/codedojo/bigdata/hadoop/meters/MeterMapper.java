package codedojo.bigdata.hadoop.meters;// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

public class MeterMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value,
                    OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        String line = value.toString();

        if (!"".equals(line)) {
            StringTokenizer st = new StringTokenizer(line, ";");
            String stationID = st.nextToken();
            for (int i = 1; i < 7; i++) {
                String str = st.nextToken();//skip some data
                System.out.println("MeterMapper.map" + str);
            }
            String meterValue = st.nextToken();
            meterValue = meterValue.replaceAll(",", ".");
            output.collect(new Text(stationID), new IntWritable(new Double(meterValue).intValue()));
        }

    }
}
