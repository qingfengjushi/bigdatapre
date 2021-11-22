package com.aa.mr.flow.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by 35267 on 2021/11/20.
 */
public class PhoneNumPartitioner extends Partitioner<LongWritable, Text> {
    @Override
    public int getPartition(LongWritable key, Text value, int i) {
        String[] split = value.toString().split("\t");
        String phoneNum = split[1];
        String prefix = phoneNum.substring(0, 3);
        if ("135".equals(prefix)) {
            return 0;
        } else if ("136".equals(prefix)) {
            return 1;
        } else if ("137".equals(prefix)) {
            return 2;
        }
        return 3;
    }
}
