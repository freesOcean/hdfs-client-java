package com.zd.hadoop.MyInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/2 0002 07:53
 * @description:
 */
public class WholeFileReader extends RecordReader<Text, BytesWritable> {
    FileSplit split;
    Configuration configuration ;
    Text k = new Text();
    BytesWritable v = new BytesWritable();

    boolean isProcess = true;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    /**
     * 核心业务逻辑
     * @return 返回false表示一次读取完毕，。true表示继续处理，对应Mapper的run方法是否继续下一次处理。
     * 一个切片会new 一个WholeFileReader
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if(isProcess) {
            //1.获取文件系统对象
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(configuration);

            //2.获取输入流
            FSDataInputStream fis = fs.open(file);

            //3.拷贝文件数据到缓冲区
            byte[] buf = new byte[(int) split.getLength()];
            IOUtils.readFully(fis, buf, 0, buf.length);

            //4.封装k
            k.set(file.toString());
            //5.封装v
            v.set(buf, 0, buf.length);

            //6.关闭资源
            IOUtils.closeStream(fis);

            isProcess = false;

            return true;
        }


        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
