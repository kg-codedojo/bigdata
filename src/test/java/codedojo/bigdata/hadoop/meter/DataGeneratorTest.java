package codedojo.bigdata.hadoop.meter;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Created By: KonstantinG
 * Date,time: 9/10/12, 5:08 PM
 *
 * 15 min
 *
 *
 *
 */
public class DataGeneratorTest {
    @Test
    public void testDataGenerator(){
        assertThat(line).contains("0;6;18;83;761;14297,4;3574,35");
    }
}
