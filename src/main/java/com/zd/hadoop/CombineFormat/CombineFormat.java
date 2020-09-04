package com.zd.hadoop.CombineFormat;

import com.zd.hadoop.TextFormat.TextInputFormatMapper;
import com.zd.hadoop.TextFormat.TextInputFormatReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
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
public class CombineFormat {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"f:/zx/input_combine","f:/zx/output3"};
        Configuration conf = new Configuration();
        //设置读取分隔符，默认是\t
//        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPARATOR," ");
        //1.获取job对象
        Job job = Job.getInstance(conf);
        //设置切片类型，不设置默认使用TextInputFormat
        //20M = 20971520
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,20971520);

        //2.设置jar存储位置
        job.setJarByClass(CombineFormat.class);

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

        //7.提交job
        boolean result =job.waitForCompletion(true);

        System.exit(result?0:1);




    }
}
