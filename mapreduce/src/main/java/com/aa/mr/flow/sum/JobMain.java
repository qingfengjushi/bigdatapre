package com.aa.mr.flow.sum;

import com.aa.mr.combiner.MyCombiner;
import com.aa.mr.combiner.WordMapper;
import com.aa.mr.combiner.WordReducer;
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
 * @Date 2021/11/16 22:27
 * @Project dp11
 * @Package com.aa.mr.flow.sum
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化一个Job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "flowcount");

        //二、设置Job对象的相关的信息。 里面包含了8个小步骤
        //1、 设置输入路径，让程序找到源文件的位置
        TextInputFormat.addInputPath(job,new Path("C:\\input\\flow.log"));

        //2、设置Mapper类型，并设置k2 v2
        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //7、设置Reducer类型，并设置 k3 v3
        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //8、设置输出路径，让结果输出到某个地方去
        TextOutputFormat.setOutputPath(job,new Path("C://wordout_flowcount"));

        //三、等待程序完成（任务的提交）
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }
}
