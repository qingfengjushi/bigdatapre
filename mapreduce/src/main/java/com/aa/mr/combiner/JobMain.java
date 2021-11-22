package com.aa.mr.combiner;

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
 * @Date 2021/11/12 22:16
 * @Project dp11
 * @Package com.aa.mr
 * 主类： 将Mapper和Reducer两个阶段串联起来，提供程序运行的入口
 */
public class JobMain {
    /**
     * 程序入口
     * 其中用一个Job对象类管理这个程序运行的很多参数
     * 指定用哪些类作为mapper的业务处理类....
     * ....
     * 其他的各种参数
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化一个Job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Combiner");

        //二、设置Job对象的相关的信息。 里面包含了8个小步骤
        //1、 设置输入路径，让程序找到源文件的位置
        TextInputFormat.addInputPath(job,new Path("D:\\input\\test1.txt"));

        //2、设置Mapper类型，并设置k2 v2
        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //3 4 5 6 四个步骤，Shuffle阶段，现在使用默认即可

        //设置一下Combiner
        job.setCombinerClass(MyCombiner.class);

        //7、设置Reducer类型，并设置 k3 v3
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //8、设置输出路径，让结果输出到某个地方去
        TextOutputFormat.setOutputPath(job,new Path("D://wordout_combiner"));

        //三、等待程序完成（任务的提交）
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }
}
