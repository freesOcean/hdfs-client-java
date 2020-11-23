package com.zd.hadoop.Partition;

import com.zd.hadoop.flowcount.FlowBean;
import com.zd.hadoop.flowcount.FlowCountMapper;
import com.zd.hadoop.flowcount.FlowCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		args = new String[]{"f:/zx/input/phone_data_s.txt","f:/zx/output3"};

        Configuration conf = new Configuration();
        // 1 获取job对象
        Job job = Job.getInstance(conf );

        // 2 设置jar的路径
        job.setJarByClass(PartitionDriver.class);

        // 3 关联mapper和reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 4 设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5 设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置分区信息
		job.setPartitionerClass(ProvincePartitioner.class);
		job.setNumReduceTasks(5);

        // 6 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交job
        boolean result = job.waitForCompletion(true);


        System.exit(result?0 :1);
    }
}
