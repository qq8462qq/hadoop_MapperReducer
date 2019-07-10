package cn.itcast.mapreduce.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/*
* 读取文件把文件设为k1和v1
* k1值为偏移量为Longwritable;v1为Text类型(文本)
* */

public class WordCountMap extends Mapper<LongWritable,Text,Text,LongWritable> {
    //把k1和v1的值转成k2和v2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();//把v1的值转字符串
        String[] split = line.split(",");//把v1以","号进行分割
        for (String word : split) {
            //把k2和v2赋给context上下文对象
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
