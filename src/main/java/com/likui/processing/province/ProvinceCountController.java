package com.likui.processing.province;

import com.likui.processing.constants.HdfsConstants;
import com.likui.processing.hdfs.HdfsSystem;
import com.likui.processing.province.mapper.ProvinceCountMapper;
import com.likui.processing.province.reduce.ProvinceCountReduce;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;

/**
 * @Auther: likui
 * @Date: 2019/8/12 20:51
 * @Description: 省份访问量统计
 */
@RestController
public class ProvinceCountController {

    @RequestMapping("/getProvinceCount")
    public boolean webMonitorApp() throws Exception {
        FileSystem fileSystem = HdfsSystem.getFileSystem();
        Job job = HdfsSystem.getJob();

        job.setJarByClass(ProvinceCountController.class);

        job.setMapperClass(ProvinceCountMapper.class);
        job.setReducerClass(ProvinceCountReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        File file = new File(HdfsConstants.FILE_PATH);

        Path inputPath = new Path(HdfsConstants.FILE_PATH);
        Path outputPath = new Path(HdfsConstants.UPLOAD_PROVINCE + file.getName());
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("上传文件系统的路径已存在，删除该路径");
        }
        fileSystem.copyFromLocalFile(inputPath, outputPath);
        FileInputFormat.setInputPaths(job, outputPath);

        Path pvCountResult = new Path(HdfsConstants.OUT_PUT_PROVINCE_RESULT);

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
