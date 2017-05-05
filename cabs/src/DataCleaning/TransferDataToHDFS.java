import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
public class TransferDataToHDFS extends Configured implements Tool {

    public static final String HADOOP_FS = "fs.defaultFS";

    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("TransferDataToHDFS <local_file_path> <HDFS_Dir_Path>");

            return 1;
        }

        String inpPath = args[0];
        Path opPath = new Path(args[1]);

        Configuration conf = getConf();
        System.out.println("Configured  the file system : " + conf.get(HADOOP_FS));
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(opPath)) {
            System.err.println("Please Run with a new path.Output Path Exists.");
            return 1;
        }
        OutputStream out = fs.create(opPath);
        InputStream inp = new BufferedInputStream(new FileInputStream(
                inpPath));
        IOUtils.copyBytes(inp, out, conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new TransferDataToHDFS(), args);
        System.exit(returnCode);
    }
}
