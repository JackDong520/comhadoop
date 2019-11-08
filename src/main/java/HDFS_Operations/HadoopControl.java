package HDFS_Operations;

import config.HadoopConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.io.File;
import java.io.IOException;

public class HadoopControl {

    private Configuration conf;
    private FileSystem fs;
    private DistributedFileSystem hdfs;


    public static void main(String[] args) throws IOException {
        HadoopControl hadoopControl = new HadoopControl();
        hadoopControl.init();
        hadoopControl.mkHDFSDir();
    }

    public HadoopControl() throws IOException {
        init();
    }

    public void init() throws IOException {
        conf = new Configuration();
        conf.set(HadoopConfig.fsDefaultName, HadoopConfig.hdfsAddress);
        fs = FileSystem.get(conf);
        hdfs = (DistributedFileSystem) fs;

    }

    public void copyToHDFS() throws IOException {
        //源文件存放在F盘下，文件名为word.txt
        Path source = new Path("f:" + File.separator + "word.txt");
        //生成的目标文件放在hdfs的/user/hadoop/20170722目录下，文件名为word.txt
        Path dst = new Path(("/user/hadoop/20170722/word.txt"));
        fs.copyFromLocalFile(source, dst);
    }

    public void getHDFSNodes() throws IOException {
        //获取所有的节点
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
        //循环打印
        for (int i = 0; i < dataNodeStats.length; i++) {
            System.out.println("DataNode_" + i + "_Name:" + dataNodeStats[i].getHostName());
        }
    }

    public void getFileLocal() throws IOException {
        //获取HDFS中/user/hadoop/20170722/word.txt文件信息
        FileStatus fileStatus = fs.getFileStatus(new Path("/user/hadoop/20170722/word.txt"));
        BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        int blockLen = blockLocations.length;
        for (int i = 0; i < blockLen; i++) {
            String[] hosts = blockLocations[i].getHosts();
            System.out.println("block_" + i + "_locations:" + hosts[0]);
        }
    }

    public void mkHDFSDir() throws IOException {

        fs.mkdirs(new Path("/user/hadoop/20191108"));

        //step4
        FileStatus fileStatus = fs.getFileStatus(new Path("/user/hadoop/20191108"));
        System.out.println(fileStatus.getPath());

    }
}
