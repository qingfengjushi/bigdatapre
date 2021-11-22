package com.aa.mr.flow.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class FlowSortReducer extends Reducer<FlowBean, Text, Text, NullWritable> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(), NullWritable.get());
    }
}
