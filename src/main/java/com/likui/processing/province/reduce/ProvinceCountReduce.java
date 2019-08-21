package com.likui.processing.province.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: likui
 * @Date: 2019/8/12 20:59
 * @Description: 省份访问量统计自定义Reduce
 */
public class ProvinceCountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for (LongWritable value : values) {
            count ++;
        }
        context.write(key, new LongWritable(count));
    }
}
