package com.zd.hadoop.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/8 0008 10:33
 * @description:
 */
public class CacheDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        args = new String[]{"f:/zx/inputTable2/order.txt", "f:/zx/output6"};

        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置jar包
        job.setJarByClass(CacheDriver.class);

        //3.关联Mapper：忽略Reducer阶段
        job.setMapperClass(CacheMapper.class);

        //4.设置最终输入:这里不再设置Mapper阶段的输入输出，Mapper后就直接输出。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.addCacheFile(new URI("file:///f:/zx/inputTable/pd.txt"));

        //因为不需要Reducer阶段，所以直接设置ReducerTask数量为0
        job.setNumReduceTasks(0);

        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);

    }
}
