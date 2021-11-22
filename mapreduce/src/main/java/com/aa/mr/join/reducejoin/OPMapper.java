package com.aa.mr.join.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author AA
 * @Date 2021/9/15 16:05
 * @Project bigdatapre
 * @Package com.aa.join.reducejoin
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN: 偏移量 的类型
 *     VALUEIN: 一行文本 的类型
 *     KEYOUT: 产品id  的类型
 *     VALUEOUT:OrderProductBean
 */
public class OPMapper extends Mapper<LongWritable, Text, Text, OrderProductBean> {
    private String filename;
    private Text keyout = new Text();
    private OrderProductBean valueout = new OrderProductBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //1、获取对应的文件的名称
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        filename = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text keyText = new Text();
        if (filename.contains("order")) {
            String[] split = value.toString().split("\t");
            keyout.set(split[1]);
            valueout.setType("order");
            valueout.setOrderid(split[0]);
            valueout.setProductid(split[1]);
            valueout.setAmount(Integer.parseInt(split[2]));
            valueout.setProductname("");
        } else if (filename.contains("product")) {
            String[] split = value.toString().split("\t");
            keyout.set(split[0]);
            valueout.setOrderid("");
            valueout.setType("product");
            valueout.setProductid(split[0]);
            valueout.setProductname(split[1]);
            valueout.setAmount(0);
        }
        context.write(keyout, valueout);
    }
}
