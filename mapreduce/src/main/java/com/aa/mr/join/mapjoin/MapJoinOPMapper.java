package com.aa.mr.join.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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
public class MapJoinOPMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text keyout = new Text();
    private Map<String,String> hashmap = new HashMap<String,String>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //1、在任务开始前面，将小表的数据，也就是product表的数据缓存到hashmap中
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        //2、通过文件流对象，读取输入文件
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream fsDataInputStream = fileSystem.open(path);

        //3、流包装转换为reader,方便按行读取
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream, "UTF-8"));

        //4、具体的按行处理
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            String[] split = line.split("\t");
            hashmap.put(split[0],split[1]);
        }
        //关流
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //5、读取大表数据order
        String[] split = value.toString().split("\t");

        //6、通过order大表数据里面的productid去小表里面取出来productname
        String productname = hashmap.get(split[1]);

        //7、拼接想要的结果
        //keyout.set(split[0] + "\t" + split[1] + "\t" + split[2] + "\t" +productname);
        keyout.set(value + "\t" +productname);

        //8、输出
        context.write(keyout,NullWritable.get());
    }
}
