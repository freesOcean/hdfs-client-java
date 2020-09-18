package com.zd.hadoop.cache;

import com.zd.hadoop.ReduceJoin.OrderPBBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/8 0008 10:27
 * @description:
 */
public class CacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    HashMap<String,String> pdMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //读取缓存的小表
        URI[] cacheFiles =context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"GBK"));

        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){
            String []fields =line.split("\t");
            pdMap.put(fields[0],fields[1]);
        }
        IOUtils.closeStream(reader);
    }

    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //		id	pid	amount
        //		1001	01	1
        //		pid	pname
        //		01	小米
        //1.分隔
        String line = new String(value.getBytes(),0,value.getLength(),"GBK");
        String []fields = line.split("\t");

        //2.获取产品名称
        String pname = pdMap.get(fields[1]);
        //2.拼接结果
        line = line + "\t" + pname;
        //3.输出
        k.set(line);

        context.write(k,NullWritable.get());

    }
}
