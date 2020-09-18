package com.zd.hadoop.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/10 0010 16:53
 * @description:
 */
public class TwoMapper extends Mapper<LongWritable, Text,Text,Text> {


    Text k = new Text();
    Text v = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String []fields = line.split("\t");

        String []names = fields[0].split("-->");
        k.set(names[0]);
        v.set(names[1]+"-->"+fields[1]);

        context.write(k,v);

    }
}
