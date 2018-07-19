package cn.itcast.stormAsk;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class MyBolt1 extends BaseRichBolt {
    private OutputCollector collector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector=collector;
    }

    public void execute(Tuple tuple) {
        String sentence=tuple.getString(0);
        String [] words=sentence.split(" ");
        for (String word:words){
            word = word.trim();
            if(!word.isEmpty()){
                word=word.toLowerCase();
                collector.emit(tuple,new Values(word));
            }
        }
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
