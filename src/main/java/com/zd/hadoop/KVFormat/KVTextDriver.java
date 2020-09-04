package com.zd.hadoop.KVFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/1 0001 11:28
 * @description:
 * 统计文档中以某个单词开头的行数。
 * KV
 */
public class KVTextDriver  {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"f:/zx/input/kv.txt","f:/zx/output6"};
        //0.获取配置信息，并配置
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPARATOR," ");
        Job job = Job.getInstance(conf);

        //默认是采用TextInputFormat，是读取的每一行内容，key是偏移量。KeyValueTextInputFormat是读取第一个单词（分隔符默认为\t），作为key，后边的内容作为value。
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        
        //1.设置jar Set the Jar by finding where a given class came from
        job.setJarByClass(KVTextDriver.class);

        //2.设置Mapper和Reduce
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReduce.class);

        //3.设置Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //4.设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //5.设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //6.提交任务
        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);
    }
}
