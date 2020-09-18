package com.zd.hadoop.topN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * hadoop序列化对象
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;// 上行流量
    private long downFlow;// 下行流量
    private long sumFlow;// 总流量

    // 空参构造， 为了后续反射用
    public FlowBean() {
        super();
    }

    public FlowBean(long upFlow, long downFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        sumFlow = upFlow + downFlow;
    }

    // 序列化方法
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    // 反序列化方法
    @Override
    public void readFields(DataInput in) throws IOException {
        // 必须要求和序列化方法顺序一致
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void set(long upFlow2, long downFlow2) {

        upFlow = upFlow2;
        downFlow = downFlow2;
        sumFlow = upFlow2 + downFlow2;

    }

    //降序排序
    @Override
    public int compareTo(FlowBean o) {
        if(this.sumFlow > o.getSumFlow()){
            return -1;
        }else if(this.sumFlow < o.getSumFlow()){
            return 1;
        }else{
            return 0;
        }
    }
}
