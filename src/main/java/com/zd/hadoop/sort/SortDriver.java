package com.zd.hadoop.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/3 0003 09:28
 * @description:
 */
public class SortDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"f:/zx/outputPhone","f:/zx/output7"};

        //1.配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置jar和Map 和Reduce
        job.setJarByClass(SortDriver.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReduce.class);

        //3.设置Map和输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setNumReduceTasks(2);

        //设置分区信息
        job.setPartitionerClass(PhonePartitioner.class);
        job.setNumReduceTasks(5);


        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //4.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //5.提交任务

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
