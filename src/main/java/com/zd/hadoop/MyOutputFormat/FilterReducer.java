package com.zd.hadoop.MyOutputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/7 0007 15:34
 * @description:
 */
public class FilterReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    Text k = new Text();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String line = key.toString();
        line = line+"\r\n";

        //重复的日志。
        for (NullWritable nullwritable: values
             ) {
            k.set(line);
            context.write(k,NullWritable.get());
        }
    }
}
