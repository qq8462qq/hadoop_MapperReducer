package cn.itcast.mapreduce.flow_count_partiton_demo3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowCountPartition extends Partitioner<Text,FlowBean> {
    /*指定分区规则
    135 开头数据到一个分区文件
    136 开头数据到一个分区文件
    137 开头数据到一个分区文件
    其他分区
    参数
    text:k2
    flowBean:v2
    i:分区个数
    */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //1获取手机号
        String phoneNum = text.toString();
        //2判断手机号以什么开头,返回对应的分区编号(0-3)
        if (phoneNum.startsWith("135")){
            return 0;
        }else if (phoneNum.startsWith("136")){
            return 1;
        }else if (phoneNum.startsWith("137")){
            return 2;
        }else {
            return 3;
        }
    }
}
