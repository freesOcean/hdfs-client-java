package com.zd.hadoop.topN;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/11 0011 09:03
 * @description:
 */
public class TopNReduce extends Reducer<FlowBean, Text,Text, FlowBean> {

    TreeMap<FlowBean,Text> treeMap = new TreeMap<>();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text text:values){
            FlowBean flowBean = new FlowBean();

            flowBean.setUpFlow(key.getUpFlow());
            flowBean.setDownFlow(key.getDownFlow());
            flowBean.setSumFlow(key.getSumFlow());

            treeMap.put(flowBean,new Text(text));

            if(treeMap.size()>10){
                treeMap.remove(treeMap.lastKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> iterator = treeMap.keySet().iterator();

        while(iterator.hasNext()){
            FlowBean v  = iterator.next();
            context.write(treeMap.get(v),v);
        }
    }
}
