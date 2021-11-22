package com.aa.mr.flow.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "flowpartitioner");
        TextInputFormat.addInputPath(job,new Path("C:\\input\\flow.log"));
        job.setMapperClass(FlowPartitionerMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置分区
        job.setPartitionerClass(PhoneNumPartitioner.class);

        job.setReducerClass(FlowPartitionerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(4);
        //设置输出路径，让结果输出到某个地方去
        TextOutputFormat.setOutputPath(job,new Path("C://wordout_flowpartitioner"));

        //等待程序完成（任务的提交）
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }
}
