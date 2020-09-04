package com.zd.hadoop.group;

import com.zd.hadoop.sort.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/4 0004 07:46
 * @description:
 */
public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        int random = new Random().nextInt(100);
        args = new String[]{"f:/zx/input/order.txt","f:/zx/output"+random};

        //1.配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置jar和Map 和Reduce
        job.setJarByClass(OrderDriver.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        //3.设置Map和输出类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置分区信息
//        job.setPartitionerClass(PhonePartitioner.class);
//        job.setNumReduceTasks(5);

        job.setGroupingComparatorClass(OrderSortComparator.class);

        //4.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //5.提交任务
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
