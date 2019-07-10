package cn.itcast.mapreduce.filow_count_demo1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReduce extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1遍历集合,并将集合集合对应字段累加
        Integer upFlow = 0;//上行数据包
        Integer downFlow = 0;//下行数据包
        Integer upCountFlow = 0;//上行流量总和
        Integer downCountFlow = 0;//下行流量总和
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        //创建FlowBean对象,给对象赋值
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);
        //写入上下文
        context.write(key, flowBean);
    }
}