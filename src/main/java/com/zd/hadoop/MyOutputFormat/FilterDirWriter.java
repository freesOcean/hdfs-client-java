package com.zd.hadoop.MyOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/7 0007 10:41
 * @description:
 */
public class FilterDirWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream eurekaFS;
    FSDataOutputStream otherFS;

    public FilterDirWriter(TaskAttemptContext job) {

        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            eurekaFS = fs.create(new Path("F:/zx/output_eureka.log"));
            otherFS = fs.create(new Path("F:/zx/output_other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if(key.toString().contains("eureka")){
            eurekaFS.write(key.toString().getBytes());
        }else{
            otherFS.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(eurekaFS);
        IOUtils.closeStream(otherFS);
    }
}
