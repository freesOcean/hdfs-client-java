package com.zd.hadoop.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/3 0003 09:17
 * @description: 以FlowBean的第一次MR结果为输入，所以不会出现重复value
 */
public class SortReduce extends Reducer<FlowBean, Text,Text,FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text v:values
             ) {
            context.write(v,key);
        }
    }
}
