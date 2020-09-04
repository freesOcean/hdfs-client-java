package com.zd.hadoop.Partition;

import com.zd.hadoop.flowcount.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 这里的key 和Value是Mapper的输出的key和value   即：Text ,FlowBean
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean>{

    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        // key是手机号
        // value 流量信息

        // 获取手机号前三位
        String prePhoneNum = key.toString().substring(0, 3);

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
