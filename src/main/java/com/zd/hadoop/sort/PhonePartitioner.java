package com.zd.hadoop.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 安照手机号前三位，分区
 */
public class PhonePartitioner extends Partitioner<FlowBean, Text>{

    @Override
    public int getPartition(FlowBean key, Text value, int numPartitions) {
        // key是手机号
        // value 流量信息

        // 获取手机号前三位
        String prePhoneNum = value.toString().substring(0, 3);

        int partition = 4;

        if ("135".equals(prePhoneNum)) {
            partition = 0;
        }else if ("136".equals(prePhoneNum)) {
            partition = 1;
        }else if ("137".equals(prePhoneNum)) {
            partition = 2;
        }else if ("138".equals(prePhoneNum)) {
            partition = 3;
        }

        return partition;
    }

}
