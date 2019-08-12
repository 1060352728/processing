package com.likui.processing.hdfs;

import com.likui.processing.constants.HdfsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;

import java.net.URI;
/**
 * @Auther: likui
 * @Date: 2019/8/9 20:27
 * @Description: 连接hdfs，获取FileSystem
 */
public class HdfsSystem {

    // 获取Configuration
    private static Configuration getConfiguration() {
        System.setProperty("hadoop.home.dir", HdfsConstants.HADOOP_HOME_DIR);
        System.setProperty("HADOOP_USER_NAME", HdfsConstants.HADOOP_USER_NAME);
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", HdfsConstants.FS_DEFAULTFS);
        return configuration;
    }

    // 获取FileSystem
    public static FileSystem getFileSystem() throws Exception {
        Configuration configuration = getConfiguration();
        return FileSystem.get(new URI(HdfsConstants.FS_DEFAULTFS),configuration,HdfsConstants.HADOOP_USER_NAME);
    }

    // 获取job
    public static Job getJob() throws Exception {
        Configuration configuration = getConfiguration();
        return Job.getInstance(configuration);
    }
}
