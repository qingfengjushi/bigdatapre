package com.aa.mr.flow.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowPartitionerReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(), NullWritable.get());
    }
}
