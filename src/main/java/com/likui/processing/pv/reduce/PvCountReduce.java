package com.likui.processing.pv.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: likui
 * @Date: 2019/8/9 20:53
 * @Description: 总访问量统计，自定义Reduce
 */
public class PvCountReduce extends Reducer<Text, LongWritable, NullWritable, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (LongWritable value : values) {
            count++;
        }
        context.write(NullWritable.get(), new LongWritable(count));
    }
}
