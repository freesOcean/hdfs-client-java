package com.zd.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * HDFS IO流相关操作
 */
public class HDFSIO {


	/**
	 * 通过流 上传文件到HDFS
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException{
		
		// 1 获取对象
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf , "xiaomao");
		
		// 2 获取输入流
		FileInputStream fis = new FileInputStream(new File("f:/测试文件.docx"));
		
		// 3 获取输出流
		FSDataOutputStream fos = fs.create(new Path("/zx/622/测试文件.docx"));
		
		// 4 流的对拷
		IOUtils.copyBytes(fis, fos, conf);
		
		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}


	/**
	 * 通过流下载hdfs上的文件到本地
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	@Test
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{
		
		// 1 获取对象
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf , "xiaomao");
		
		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/zx/622/测试文件.docx"));
		
		// 3 获取输出流
		FileOutputStream fos = new FileOutputStream(new File("f:/download.docx"));
		
		// 4 流的对拷
		IOUtils.copyBytes(fis, fos, conf);
		
		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
	
	//下载第一块
	@Test
	public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{
		
		// 1 获取对象
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf , "xiaomao");
		
		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/zx/622/测试文件.docx"));
		
		// 3 获取输出流
		FileOutputStream fos = new FileOutputStream(new File("f:/测试文件.docx.part1"));
		
		// 4 流的对拷（只拷贝20m）
		byte[] buf = new byte[1024];
		for (int i = 0; i < 1024 * 20; i++) {
			fis.read(buf);
			fos.write(buf);
		}
		
		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
	
	// 下载第二块
	@SuppressWarnings("resource")
	@Test
	public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{
		
		// 1 获取对象
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf , "xiaomao");
		
		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/zx/622/测试文件.docx"));
		
		// 3 设置指定读取的起点
		fis.seek(1024*1024*20);
		
		// 4 获取输出流
		FileOutputStream fos = new FileOutputStream(new File("f:/测试文件.docx.part2"));
		
		// 5 流的对拷
		IOUtils.copyBytes(fis, fos, conf);
		
		// 6 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
	
	
	
	
	
	
	
}
