package com.zd.hadoop.ReduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class OrderPBReducer extends Reducer<IntWritable, OrderPBBean, OrderPBBean, NullWritable> {
	
	@Override
	protected void reduce(IntWritable key, Iterable<OrderPBBean> values,
                          Context context) throws IOException, InterruptedException {
		
		// 存储所有订单集合
		ArrayList<OrderPBBean> orderBeans = new ArrayList<>();
		// 存储产品信息
		OrderPBBean pdBean = new OrderPBBean();

		for (OrderPBBean tableBean : values) {

			if ("order".equals(tableBean.getFlag())) {// 订单表

				OrderPBBean tmpBean = new OrderPBBean();

				try {
					BeanUtils.copyProperties(tmpBean, tableBean);

					orderBeans.add(tmpBean);

				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}else {
				try {
					BeanUtils.copyProperties(pdBean, tableBean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}

		for (OrderPBBean tableBean : orderBeans) {
			tableBean.setPname(pdBean.getPname());

			context.write(tableBean, NullWritable.get());
		}


		//错误写法，不能直接将引用加入集合，因为Reducer阶段 后，会释放对象，所以应该采用拷贝对象的方式
//		for (OrderPBBean order:values
//			 ) {
//			if(order.getFlag().equals("order")){
//				orderBeans.add(order);
//			}else{
//				pdBean = order;
//			}
//		}
//
//		for (OrderPBBean tableBean : orderBeans) {
//			tableBean.setPname(pdBean.getPname());
//
//			context.write(tableBean, NullWritable.get());
//		}

	}
}
