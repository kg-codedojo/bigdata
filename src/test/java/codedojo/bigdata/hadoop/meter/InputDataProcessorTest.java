package codedojo.bigdata.hadoop.meter;

import codedojo.bigdata.hadoop.meters.InputDataProcessor;
import org.junit.Test;

import java.io.*;
import java.math.BigDecimal;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Created By: KonstantinG
 * Date,time: 9/9/12, 10:01 PM
 */
public class InputDataProcessorTest {
    @Test public void testPreprocessData() throws Exception {
        InputDataProcessor processor = new InputDataProcessor("data/preprocessed");
        processor.preprocess("data/fdata.csv","fdata.csv","stationID1");
        FileInputStream in = new FileInputStream("data/preprocessed/fdata.csv");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new DataInputStream(in)));
        String line = bufferedReader.readLine();
        in.close();
        assertThat(line).contains("stationID1");
    }
}
