package com.zd.hadoop.topN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/11 0011 07:50
 * @description: 按照总流量降序排序
 */
public class TopNMapper extends Mapper<LongWritable ,Text,FlowBean, Text> {

    private FlowBean k;
    private TreeMap<FlowBean, Text> treeMap = new TreeMap<FlowBean, Text>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //13560436662	112	954	1066

        k = new FlowBean();
        Text v = new Text();
        String line = value.toString();

        //1.切分
        String []fields = line.split("\t");

        //2.封装
        k.setUpFlow(Long.parseLong(fields[1]));
        k.setDownFlow(Long.parseLong(fields[2]));
        k.setSumFlow(Long.parseLong(fields[3]));

        v.set(fields[0]);

        //value是手机号
        treeMap.put(k,v);

        //因为key是FlowBean,是按降序排序，所以最后一个是总流量最低的用户。TreeMap默认会按照key的升序排序。
        if(treeMap.size()>10){
            treeMap.remove(treeMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> iterator = treeMap.keySet().iterator();

        while(iterator.hasNext()){
            FlowBean k = iterator.next();
            context.write(k,treeMap.get(k));
        }

    }

}
