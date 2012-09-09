package codedojo.bigdata.hadoop.utils;

/**
 * Created By: KonstantinG
 * Date,time: 9/9/12, 8:17 PM
 */
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtil {

//    private String coreSiteXml;
//    private String coreHDFSXml;
    private Configuration conf = new Configuration();

    public HDFSUtil(String hadoopHome) {
        this(hadoopHome + "/conf/core-site.xml", hadoopHome + "/conf/hdfs-site.xml");
    }

    public HDFSUtil(String coreSiteXml, String coreHDFSXml) {
        conf.addResource(new Path(coreSiteXml));
        conf.addResource(new Path(coreHDFSXml));
    }

    public void addFile(String source, String dest) throws IOException {
//        Configuration conf = new Configuration();

        // Conf object will read the HDFS configuration parameters from these
        // XML files.
//        conf.addResource(new Path(coreSiteXml));
//        conf.addResource(new Path(coreHDFSXml));

        FileSystem fileSystem = FileSystem.get(conf);

        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1,
                source.length());

        // Create the destination path including the filename.
        if (dest.charAt(dest.length() - 1) != '/') {
            dest = dest + "/" + filename;
        } else {
            dest = dest + filename;
        }

        // System.out.println("Adding file to " + destination);

        // Check if the file already exists
        Path path = new Path(dest);
        if (fileSystem.exists(path)) {
            System.out.println("File " + dest + " already exists");
            return;
        }

        // Create a new file and write data to it.
        FSDataOutputStream out = fileSystem.create(path);
        InputStream in = new BufferedInputStream(new FileInputStream(
                new File(source)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        // Close all the file descripters
        in.close();
        out.close();
        fileSystem.close();
    }

    public void readFile(String file, String outputFile) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        FSDataInputStream in = fileSystem.open(path);

//        String filename = file.substring(file.lastIndexOf('/') + 1,
//                file.length());

        OutputStream out = new BufferedOutputStream(new FileOutputStream(
                new File(outputFile)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
        fileSystem.close();
    }

    public void deletePath(String file) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        fileSystem.delete(new Path(file), true);

        fileSystem.close();
    }

    public void mkdir(String dir) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(dir);
        if (fileSystem.exists(path)) {
            System.out.println("Dir " + dir + " already not exists");
            return;
        }

        fileSystem.mkdirs(path);

        fileSystem.close();
    }

}
