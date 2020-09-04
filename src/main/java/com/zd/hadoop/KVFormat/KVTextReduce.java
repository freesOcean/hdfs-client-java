package com.zd.hadoop.KVFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/8/31 0031 19:19
 * @description:
 *
 * 统计第一单词相同的行的数量
 */
public class KVTextReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    IntWritable v = new IntWritable();
    int sum;
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        sum = 0 ;
        for (IntWritable value:values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
