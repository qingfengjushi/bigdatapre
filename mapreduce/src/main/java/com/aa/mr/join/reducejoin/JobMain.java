package com.aa.mr.join.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/9/15 16:48
 * @Project bigdatapre
 * @Package com.aa.join.reducejoin
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        //一、初始化一个Job
        Job job = Job.getInstance(configuration, "reducejoin");
       // job.setJarByClass(JobMain.class);

        //二、设置Job的相关信息，配置   8个小步骤
        //1、设置输入的路径，让程序找到源数据的位置
        job.setInputFormatClass(TextInputFormat.class);
        //设置成本地的输入路径
      //  TextInputFormat.addInputPath(job,new Path("C:\\input\\join"));
        FileInputFormat.setInputPaths(job, new Path("C:\\input\\join"));
        //TextInputFormat.addInputPath(job,new Path("C:\\input\\join\\join_order.txt"));
        //TextInputFormat.addInputPath(job,new Path("C:\\input\\join\\join_product.txt"));
        //2、设置mapper类，并设置k2 v2
        job.setMapperClass(OPMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderProductBean.class);


        //7、设置reducer类，并设置k3 v3的类型
        job.setReducerClass(OPReducer.class);
        job.setOutputKeyClass(OrderProductBean.class);
        job.setOutputValueClass(NullWritable.class);

        //8、设置输出的路径
        job.setOutputFormatClass(TextOutputFormat.class);
        //TextOutputFormat.setOutputPath(job,new Path("C://reducejoinout"));
        //FileOutputFormat.setOutputPath(job,new Path("C://reducejoinout"));
        FileOutputFormat.setOutputPath(job,new Path("C://reducejoinout"));
        //三、等待完成
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
    //    System.exit(b ? 0:1);
    }
}
