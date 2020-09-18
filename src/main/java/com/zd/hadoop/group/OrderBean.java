package com.zd.hadoop.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/3 0003 15:22
 * @description: 封装订单的编号和金额
 */
public class OrderBean implements WritableComparable<OrderBean> {


    private int orderId;
    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(int orderId,double price){
        this.orderId = orderId;
        this.price = price;
    }

    //返回1,表示不swap
    @Override
    public int compareTo(OrderBean o) {
        //先按照订单编号升序排序，如果订单编号一致，再按金额降序排序
        int result;
        if(orderId<o.getOrderId()){
            result = -1;
        }else if(orderId>o.getOrderId()){
            result = 1;
        }else{

            if(price>o.getPrice()){
                result = -1;
            }else if(price<o.getPrice()){
                result = 1;
            }else{
                result = 0;
            }

        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readInt();
        price = dataInput.readDouble();
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }

}
