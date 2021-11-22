package com.aa.mr.sort;

import com.aa.mr.helloworld.WordMapper;
import com.aa.mr.helloworld.WordReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/11/16 21:22
 * @Project dp11
 * @Package com.aa.mr.sort
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化一个Job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "sort");

        //二、设置Job对象的相关的信息。 里面包含了8个小步骤
        //1、 设置输入路径，让程序找到源文件的位置
        TextInputFormat.addInputPath(job,new Path("C:\\input\\test3.txt"));

        //2、设置Mapper类型，并设置k2 v2
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(MySortBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //3 4 5 6 四个步骤，Shuffle阶段，现在使用默认即可

        //7、设置Reducer类型，并设置 k3 v3
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(MySortBean.class);
        job.setOutputValueClass(NullWritable.class);

        //8、设置输出路径，让结果输出到某个地方去
        TextOutputFormat.setOutputPath(job,new Path("C://wordout_sort"));

        //三、等待程序完成（任务的提交）
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }
}
