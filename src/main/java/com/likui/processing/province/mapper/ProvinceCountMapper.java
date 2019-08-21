package com.likui.processing.province.mapper;

import com.likui.processing.utils.IPParser;
import com.likui.processing.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * @Auther: likui
 * @Date: 2019/8/12 20:58
 * @Description: 省份访问量统计自定义Mapper
 */
public class ProvinceCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private LongWritable ONE = new LongWritable(1);

    private LogParser logParser;
    protected void setup(Context context) throws IOException, InterruptedException {
        logParser = new LogParser();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, String> stringMap = logParser.parse(value.toString());
        String ip = stringMap.get("ip");
        if(StringUtils.isNotBlank(ip)) {
            IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp(ip);
            if(null != regionInfo){
                String province = stringMap.get("province");
                if(StringUtils.isNotBlank(province)) {
                    context.write(new Text(province), ONE);
                }else {
                    context.write(new Text("--"), ONE);
                }
            }else {
                context.write(new Text("--"), ONE);
            }
        } else {
            context.write(new Text("--"), ONE);
        }
    }
}
