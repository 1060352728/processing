package com.likui.processing.pv.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: likui
 * @Date: 2019/8/9 20:47
 * @Description: 自定义Mapper
 */
public class PvCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text KEY = new Text("key");
    private LongWritable ONE = new LongWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(KEY, ONE);
    }
}
