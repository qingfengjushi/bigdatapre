package com.aa.mr.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author LIAO
 * @create 2020-07-29 21:00
 * 求订单最大值的主类
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化一个job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "mygroup2");
        //job.setJarByClass(JobMain.class);

        //二、配置Job信息
        //1、设置输入信息
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("C:\\input\\orders.txt"));

        //2、设置mapper 并设置k2 v2的类型
        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);

        //3 4 5 6  shuffle
        //3 分区设置
        job.setPartitionerClass(OrderPartition.class);

        //6 分组设置
        job.setGroupingComparatorClass(OrderGroup.class);

        //7、设置Reducer,并设置k3 v3 的类型
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置NumReduceTask的个数  默认是1
        //请问一下，job.setNumReduceTasks(4); ，结果文件有几个，
        //认为是3个打个3，认为是4个打4，认为还有其他可能的打其他的对应的值。
        //job.setNumReduceTasks(3);

        //8、设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("C://mygroup_out3"));

        //三、等待完成  实际上就是提交任务
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }
}
