package com.zd.hadoop.MyOutputFormat;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/7 0007 15:30
 * @description: 将读入的日志记录过滤后输出到不同的文件上
 */
public class FilterMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //过滤空行
        if(StringUtils.isNotBlank(value.toString())) {
            context.write(value, NullWritable.get());
        }else{
            context.getCounter("MAP","empty").increment(1);
        }
    }
}
