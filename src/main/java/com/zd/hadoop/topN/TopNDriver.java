package com.zd.hadoop.topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/11 0011 08:46
 * @description:
 *
 * 错误，把FlowBean的比较方法写错，导致Mapper阶段出错
 *
 */
public class TopNDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"F:/zx/outputPhone/part-r-00000","f:/zx/output11219"};

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(TopNDriver.class);

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReduce.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);
    }


}
