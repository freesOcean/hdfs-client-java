package com.zd.hadoop.ReduceJoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OrderPBMapper extends Mapper<LongWritable, Text, IntWritable, OrderPBBean> {

	String name;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {

		// 获取文件的名称
		FileSplit inputSplit = (FileSplit) context.getInputSplit();

		name = inputSplit.getPath().getName();
	}

	OrderPBBean tableBean = new OrderPBBean();
	IntWritable k = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
//		id	pid	amount
//		1001	01	1
//
//		pid	pname
//		01	小米
		
		// 1 获取一行
//		String line = value.toString();
		String line = new String(value.getBytes(),0,value.getLength(),"GBK");
		String[] fields = line.split("\t");
		if (name.startsWith("order")) {// 订单表
			

			
			// 封装key和value
			tableBean.setId(Integer.parseInt(fields[0]));
			tableBean.setPid(Integer.parseInt(fields[1]));
			tableBean.setAmount(Integer.parseInt(fields[2]));
			tableBean.setPname("");
			tableBean.setFlag("order");
			
			k.set(Integer.parseInt(fields[1]));
			
		}else {// 产品表
			
			// 封装key和value
			tableBean.setId(-1);
			tableBean.setPid(Integer.parseInt(fields[0]));
			tableBean.setAmount(0);
			tableBean.setPname(fields[1]);
			tableBean.setFlag("pd");
			
			k.set(Integer.parseInt(fields[0]));
		}
		
		// 写出
		context.write(k, tableBean);
	}
}
