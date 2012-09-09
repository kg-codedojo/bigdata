package codedojo.bigdata.hadoop.utils;

import org.junit.Test;

/**
 * Created By: KonstantinG
 * Date,time: 9/9/12, 8:19 PM
 */
public class HDFSUtilTest {
    private HDFSUtil util = new HDFSUtil("/home/master/dev/hadoop");

    @Test public void testCRUD() throws Exception {
        util.mkdir("/test/dir");
        util.addFile("data/fdata.csv", "/test/dir");
        util.readFile("/test/dir/fdata.csv","data/temp/fdata.out.csv");
        util.deletePath("/test/dir/fdata.csv");
        util.deletePath("/test");
    }
}
