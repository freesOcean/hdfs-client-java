package com.zd.hadoop.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/10 0010 16:14
 * @description:
 */
public class OneReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value:values
             ) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key,v);


    }
}
