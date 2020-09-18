package com.zd.hadoop.MyOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/7 0007 15:55
 * @description:
 * 最后，目录/zx/outputEureka中只会有一个成功的标志文件。输出文件在 FilterWriter中定义
 */
public class FilterDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"f:/zx/input/log.txt","f:/zx/outputEureka"};

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //1.设置jar包
        job.setJarByClass(FilterDriver.class);

        //2.设置Mapper和Reducer
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        //3.设置Mapper输出和最终输出的格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //4.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setOutputFormatClass(FilterDirOutputFormat.class);

        //5.提交任务
        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);

    }
}
