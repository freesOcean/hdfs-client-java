package com.zd.hadoop.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/10 0010 15:44
 * @description:
 * 多job串联
 *
 */
public class OneMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    String fileName;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();
    }

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.读取一行
        String line = new String(value.getBytes(),0,value.getLength(),"GBK");
        if(StringUtils.isEmpty(line)){
            return;
        }
        //2.按空格切分
        String[] fields = line.split(" ");
        //3.将字段和文件名称拼接后作为key
        for (String name:fields) {
            k.set(name+"-->"+fileName);
            context.write(k,v);
        }
    }
}
