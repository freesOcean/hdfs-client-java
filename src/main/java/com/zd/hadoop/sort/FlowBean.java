package com.zd.hadoop.sort;

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

    /**
     * 可以理解当前传入的对象是先前写入的对象。返回1表示不swap
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
        int result;
        // 根据总流量比较:倒序排序
        if(sumFlow> o.getSumFlow()){
            result = -1;
        }else if(sumFlow < o.getSumFlow()){
            result = 1;
        }else{
            result = 0;
        }
        return result;
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
}
