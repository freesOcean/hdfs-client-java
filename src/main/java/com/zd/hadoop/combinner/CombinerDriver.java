package com.zd.hadoop.combinner;

import com.zd.hadoop.mapreduce.WordcountMapper;
import com.zd.hadoop.mapreduce.WordcountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/8/3 0003 15:09
 * @description:
 *
 * combiner 第一种方式就是定义一个Combiner类 继承Reduce类，进行局部的汇总.
 * 因为Combiner和Reducer的区别就是执行的位置不同，Combiner和MapTask在同一个节点运行，而Reducer是
 * 汇总所有MapTask的结果，需要进行网络拷贝。
 * Combiner的作用就是减少网络传输的数据量
 *
 */
public class CombinerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"f:/zx/input/words.txt","f:/zx/output6"};
        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(CombinerDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //不设置默认根据切片决定，默认是1
//        job.setNumReduceTasks(2);

//        job.setCombinerClass(WordcountCombiner.class);

        int a = job.getNumReduceTasks();
        System.out.println("taskNum :"+a);

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
