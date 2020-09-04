package com.zd.hadoop.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/4 0004 07:35
 * @description:
 * 不关系value，所以Map输出是NullWritable
 */
public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    //0000001	Pdt_01	222.8
    //0000002	Pdt_05	722.4


    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.读取一行并切分
        String[] field = value.toString().split("\t");

        //2.封装k,v
        int orderId = Integer.parseInt(field[0]);
        double price = Double.parseDouble(field[2]);

        k.setOrderId(orderId);
        k.setPrice(price);

        //3.写出
        context.write(k,NullWritable.get());

    }
}
