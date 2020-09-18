package com.zd.hadoop.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/10 0010 17:02
 * @description:
 */
public class TwoReduce extends Reducer<Text,Text,Text,Text> {

    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();

        for(Text text:values){
            sb.append(text + " ");
        }
        v.set(sb.toString());
        context.write(key,v);
    }
}
