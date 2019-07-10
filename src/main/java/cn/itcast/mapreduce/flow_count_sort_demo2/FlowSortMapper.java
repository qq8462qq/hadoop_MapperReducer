package cn.itcast.mapreduce.flow_count_sort_demo2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable,Text,FlowBean, Text> {
//将k1 v1 转成k2v2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拆分行文本数据得到k2 v2
        String[] split = value.toString().split("\t");
        String phoneNum=split[0];//得到v2
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[1]));
        flowBean.setDownFlow(Integer.parseInt(split[2]));
        flowBean.setUpCountFlow(Integer.parseInt(split[3]));
        flowBean.setDownCountFlow(Integer.parseInt(split[4]));
        //将k2 v2写入上下文中
        context.write(flowBean,new Text(phoneNum));
    }
}
