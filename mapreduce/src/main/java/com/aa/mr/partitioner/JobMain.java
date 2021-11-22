package com.aa.mr.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/11/16 20:32
 * @Project dp11
 * @Package com.aa.mr.partitioner
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化一个Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "partitioner");

        //二、设置Job相关的信息
        //1、设置输入路径
        TextInputFormat.addInputPath(job,new Path("D://input//test2.txt"));

        //2、设置Mapper
        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //3 4 5 6
        //设置分区
        job.setPartitionerClass(MyPartitioner.class);

        //7、设置Reducer
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //注意要设置ReduceTask的个数
        //如下设置，分区里面设置的是2个，NumReduceTasks里面设置的是3个，请问结果文件中有几个 part-r-000..
        //认为是1的扣1 ，认为是2的扣2，任务是3的扣3，其他扣4
        //真实的结果是3个。
        job.setNumReduceTasks(3);

        //8、设置输出路径
        TextOutputFormat.setOutputPath(job,new Path("D://wordout_partitioner"));

        //三、等待完成（其实就是提交程序）
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }
}
