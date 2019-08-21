package com.likui.processing.hdfs;

import com.likui.processing.constants.HdfsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;

/**
 * @Auther: likui
 * @Date: 2019/8/21 20:27
 * @Description:
 */
public class MyFileSystem {

    public static FileSystem getFileSystem() throws IOException {
        System.setProperty("HADOOP_USER_NAME", HdfsConstants.HADOOP_USER_NAME);
        Configuration conf = new Configuration();
        //conf.set("hadoop.security.authentication", "kerberos"); // 设置hadoop的登录认证为kerberos,默认的配置是simple
        //System.setProperty("java.security.krb5.conf","conf/krb5.conf"); //指定kerberos的路径和文件名
        //UserGroupInformation.setConfiguration(conf);
        //UserGroupInformation.loginUserFromKeytab("shixiuru@EXAMPLE.COM", "conf/shixiuru.keytab");
        conf.addResource(new File("D:\\java_workspace\\bigdata_workspace\\processing\\src\\main\\resources\\config\\core-site.xml").toURI().toURL());
        conf.addResource(new File("D:\\java_workspace\\bigdata_workspace\\processing\\src\\main\\resources\\config\\hdfs-site.xml").toURI().toURL());
        FileSystem fileSystem = FileSystem.get(conf);
        System.out.println(conf.get("fs.default.name"));
        return fileSystem;
    }

    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = getFileSystem();
        boolean flag = fileSystem.mkdirs(new Path("/testfile"));
        System.out.println(flag);
    }
}
