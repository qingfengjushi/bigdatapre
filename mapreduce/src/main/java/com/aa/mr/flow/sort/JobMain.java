package com.aa.mr.flow.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化一个Job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "flowsort");

        //二、设置Job对象的相关的信息。 里面包含了8个小步骤
        //1、 设置输入路径，让程序找到源文件的位置
        TextInputFormat.addInputPath(job,new Path("C:\\input\\flowsort"));

        //2、设置Mapper类型，并设置k2 v2
        job.setMapperClass(FlowSortMapper.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //7、设置Reducer类型，并设置 k3 v3
        job.setReducerClass(FlowSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //8、设置输出路径，让结果输出到某个地方去
        TextOutputFormat.setOutputPath(job,new Path("C://wordout_flowsort"));

        //三、等待程序完成（任务的提交）
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }
}
