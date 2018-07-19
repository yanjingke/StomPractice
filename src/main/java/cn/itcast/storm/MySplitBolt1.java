package cn.itcast.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class MySplitBolt1 extends BaseRichBolt {
    OutputCollector collector;
    //初始化方法
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String line = tuple.getString(0);
        //String line2=tuple.getStringByField("love");
        String[] split = line.split(" ");
        for (String word : split) {
           // collector.emit(new Values(word, 1));
            collector.emit(new Values(word,1));
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "num"));
    }
}
