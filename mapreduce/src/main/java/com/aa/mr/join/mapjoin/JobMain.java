package com.aa.mr.join.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        //一、初始化一个Job
        Job job = Job.getInstance(configuration, "mapjoin");

        //二、设置Job的相关信息，配置   8个小步骤
        //1、设置输入的路径，让程序找到源数据的位置
        job.setInputFormatClass(TextInputFormat.class);
        //设置成本地的输入路径
        TextInputFormat.addInputPath(job,new Path("C://input//join//join_order.txt"));

        //2、设置mapper类，并设置k2 v2
        job.setMapperClass(MapJoinOPMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 3 4 5 6 四个步骤，shuffle阶段，使用默认的配置就可以了

        // 加载缓存数据  注意下面写路径的时候，前面必须加上file:///
        job.addCacheFile(new URI("file:///C://input//join//join_product.txt"));
        //map端join不需要reduce阶段了，所以直接设置NumReduceTasks为0 就可以关闭 Reduce 阶段
        job.setNumReduceTasks(0);

        //7、设置reducer类，并设置k3 v3的类型
        /*job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);*/

        //8、设置输出的路径
        job.setOutputFormatClass(TextOutputFormat.class);
        //TextOutputFormat.setOutputPath(job,new Path("D://reducejoinout"));
        FileOutputFormat.setOutputPath(job,new Path("C://mapjoinout"));

        //三、等待完成
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0:1);
    }
}
