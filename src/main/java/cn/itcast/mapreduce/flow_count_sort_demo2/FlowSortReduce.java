package cn.itcast.mapreduce.flow_count_sort_demo2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSortReduce extends Reducer<FlowBean, Text,Text,FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //遍历集合,取出k3,v3写入上下文
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
