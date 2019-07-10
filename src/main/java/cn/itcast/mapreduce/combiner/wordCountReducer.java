package cn.itcast.mapreduce.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/*
* 把k2和v2转成k3和v3并写入上下文中
*
* */

public class wordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
   //reduce方法是 :把新的k2和v2转成k3和v3
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //自定义我们的reduce逻辑
        //所有的key都是我们的单词，所有的values都是我们单词出现的次数
        long count=0;//定义变量用生成新的v3
        for (LongWritable value : values) {
            count+=value.get();
        }
        //把k3和v3放入context上下文中
        context.write(key,new LongWritable(count));
    }
}
