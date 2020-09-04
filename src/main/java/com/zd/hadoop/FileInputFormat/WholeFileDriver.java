package com.zd.hadoop.FileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/2 0002 08:44
 * @description:
 */
public class WholeFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"f:/zx/input_small","f:/zx/outputSmall"};

        //1.获取配置文件并配置
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //2.设置jar位置
        job.setJarByClass(WholeFileDriver.class);

        //3.设置Mapper和Reduce
        job.setMapperClass(WholeFileMapper.class);
        job.setReducerClass(WholeFileReduce.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //4.设置Mapper输入类型和最终输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //5.设置最终输出类型
        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);


        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7.提交任务
        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);



    }
}
