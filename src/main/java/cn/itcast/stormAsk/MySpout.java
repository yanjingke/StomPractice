package cn.itcast.stormAsk;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IControlSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class MySpout extends BaseRichSpout {
    private  SpoutOutputCollector collector;
    private Random random;
    private Map<String,Values>map=new HashMap<String, Values>();
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector=collector;
        this.random=random;
    }

    public void nextTuple() {
        String[] sentences = new String[]{"the cow jumped over the moon",
                "the cow jumped over the moon",
                "the cow jumped over the moon",
                "the cow jumped over the moon", "the cow jumped over the moon"};
        String sentence=sentences[random.nextInt(sentences.length)];
        String messageId= UUID.randomUUID().toString().replace("-","");
        Values tuple=new Values(sentence);
        collector.emit(tuple,messageId);
        map.put(messageId,tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
       // declarer.declare(new Fields("sentence"));
        declarer.declare(new Fields("word"));
        random=new Random();
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("处理成功:"+msgId);
        map.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("处理失败:"+msgId);
        Values tuple = map.get(msgId);
        collector.emit(tuple,msgId);
    }
}
