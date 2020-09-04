package com.zd.hadoop.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/4 0004 07:58
 * @description: 分组排序，让map 阶段的对象分为一组，一同进入Reduce
 */
public class OrderSortComparator extends WritableComparator {

    protected  OrderSortComparator(){
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        int result ;
        if(aBean.getOrderId()>bBean.getOrderId()){
            result = 1;
        }else if(aBean.getOrderId()<bBean.getOrderId()){
            result = -1;
        }else{
            result = 0;
        }

        return result;
    }
}
