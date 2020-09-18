package com.zd.hadoop.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

import static org.xerial.snappy.Snappy.compress;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/10 0010 08:39
 * @description:
 */
public class CompressFile {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

//        compressFile("F:/zx/input/phone_data.txt","org.apache.hadoop.io.compress.DefaultCodec");
//        compressFile("F:/zx/input/phone_data.txt","org.apache.hadoop.io.compress.GzipCodec");
        compressFile("F:/zx/input/phone_data.txt","org.apache.hadoop.io.compress.BZip2Codec");

    }

    private static void compressFile(String path, String codec) throws IOException, ClassNotFoundException {
        System.out.println("开始"+System.currentTimeMillis());
        //1.获取输入流
        FileInputStream fis = new FileInputStream(path);
        Class classCodec = Class.forName(codec);
        CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(classCodec,new Configuration());

        //2.获取输出流
        FileOutputStream fos = new FileOutputStream(new File(path+compressionCodec.getDefaultExtension()));
        CompressionOutputStream cos = compressionCodec.createOutputStream(fos);

        //3.流的对拷
        IOUtils.copyBytes(fis,cos,1024*1024,false);
        //4.关闭流
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        System.out.println("结束"+System.currentTimeMillis());

    }


}
