package com.zd.hadoop.ReduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderPBBean implements Writable {

//	id	pid	amount
//	pid	pname

	private int id;	// 订单id
	private int pid;	// 产品id
	private int amount;	// 数量
	private String pname;	// 产品名称
	private String flag;	// 定义一个标记，标记是订单表还是产品表

	public OrderPBBean() {
		super();
	}

	public OrderPBBean(int id, int pid, int amount, String pname, String flag) {
		super();
		this.id = id;
		this.pid = pid;
		this.amount = amount;
		this.pname = pname;
		this.flag = flag;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		// 序列化方法
		out.writeInt(id);
		out.writeInt(pid);
		out.writeInt(amount);
		out.writeUTF(pname);
		out.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// 反序列化方法
		id = in.readInt();
		pid = in.readInt();
		amount = in.readInt();
		pname = in.readUTF();
		flag = in.readUTF();
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getPid() {
		return pid;
	}

	public void setPid(int pid) {
		this.pid = pid;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	@Override
	public String toString() {
		return id + "\t" + amount + "\t" + pname;
	}
}
