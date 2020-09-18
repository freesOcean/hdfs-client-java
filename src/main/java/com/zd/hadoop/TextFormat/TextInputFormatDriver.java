package com.zd.hadoop.TextFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/8/28 0028 19:08
 * @description:
 *
 * 默认采用的是TextInputFormat， 会读取一行
 *
 *
 */
public class TextInputFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"f:/zx/input/words1.txt","f:/zx/output5"};
        Configuration conf = new Configuration();

        //开启Map阶段的压缩
        conf.setBoolean("",true);
        conf.setClass("", GzipCodec.class, CompressionCodec.class);

        //1.获取job对象
        Job job = Job.getInstance(conf);
        //2.设置jar存储位置
        job.setJarByClass(TextInputFormatDriver.class);



        //3.关联Map和Reduce类
        job.setMapperClass(TextInputFormatMapper.class);
        job.setReducerClass(TextInputFormatReducer.class);

        //4.设置Mapper阶段的输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置Reduce阶段数据输出的数据的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //设置Reducer输出压缩
        FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);

        FileInputFormat.setMaxInputSplitSize(job,256*1024*1024);

        job.setNumReduceTasks(2);



        //7.提交job
        boolean result =job.waitForCompletion(true);

        System.exit(result?0:1);




    }
}
