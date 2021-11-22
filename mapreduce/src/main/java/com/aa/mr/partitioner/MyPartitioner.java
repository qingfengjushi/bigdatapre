package com.aa.mr.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author AA
 * @Date 2021/11/16 20:24
 * @Project dp11
 * @Package com.aa.mr.partitioner
 * Partitioner<KEY, VALUE>
 *     KEY: 单词的  类型
 *     VALUE: 次数的 类型
 */
public class MyPartitioner extends Partitioner<Text, LongWritable> {
    /**
     * 需求：根据单词的长度给单词出现的次数的结果存储到不同文件中，以便于在快速查询
     * 具体的规则： 单词的长度 >=5 的在一个结果文件， 单词的长度 < 5 的在一个结果文件中。
     * @param text
     * @param longWritable
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
        //1、具体的规则实现
        if (text.toString().length() >= 5){
            return 0;
        }else {
            return 1;
        }
    }
}
