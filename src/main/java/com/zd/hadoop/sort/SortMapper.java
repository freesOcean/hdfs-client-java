package com.zd.hadoop.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/3 0003 08:49
 * @description: 默认利用TextInputFormat读取，KeyIn : LongWritable (偏移量) ,ValueIn:Text （一行内容）
 */
public class SortMapper extends Mapper<LongWritable,Text,FlowBean,Text> {

    FlowBean k = new FlowBean();
    Text v= new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //13560436662	112	954	1066
        //1.分隔一行数据
        String[] s = value.toString().split("\t");

        //2.封装对象
        String phone = s[0];
        v.set(phone);

        long upFlow = Long.parseLong(s[1]);
        long downFlow = Long.parseLong(s[2]);
        long sumFlow = Long.parseLong(s[3]);

        k.setUpFlow(upFlow);
        k.setDownFlow(downFlow);
        k.setSumFlow(sumFlow);

        //3.写出
        context.write(k,v);
    }
}
