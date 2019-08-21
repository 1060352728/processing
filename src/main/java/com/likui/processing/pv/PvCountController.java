package com.likui.processing.pv;

import com.likui.processing.constants.HdfsConstants;
import com.likui.processing.hdfs.HdfsSystem;
import com.likui.processing.pv.mapper.PvCountMapper;
import com.likui.processing.pv.reduce.PvCountReduce;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;

/**
 * @Auther: likui
 * @Date: 2019/8/9 20:45
 * @Description: 总访问量统计，将本地文件上传至HDFS，统计访问量上传HDFS
 */
@RestController
public class PvCountController {

    @RequestMapping("/getPvCount")
    public boolean webMonitorApp() throws Exception {
        FileSystem fileSystem = HdfsSystem.getFileSystem();
        Job job = HdfsSystem.getJob();

        job.setJarByClass(PvCountController.class);

        job.setMapperClass(PvCountMapper.class);
        job.setReducerClass(PvCountReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        File file = new File(HdfsConstants.FILE_PATH);

        Path inputPath = new Path(HdfsConstants.FILE_PATH);
        Path outputPath = new Path(HdfsConstants.UPLOAD_PV + file.getName());
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("上传文件系统的路径已存在，删除该路径");
        }
        fileSystem.copyFromLocalFile(inputPath, outputPath);
        FileInputFormat.setInputPaths(job, outputPath);

        Path pvCountResult = new Path(HdfsConstants.OUT_PUT_PV_RESULT);

        if(fileSystem.exists(pvCountResult)) {
            fileSystem.delete(pvCountResult, true);
            System.out.println("输出文件系统的路径已存在，删除该路径");
        }

        FileOutputFormat.setOutputPath(job, pvCountResult);

        boolean flag = job.waitForCompletion(true);
        System.out.println(flag);
        return flag;
    }
}
