package cn.itcast.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.sun.javafx.collections.MappingChange;

import java.util.HashMap;
import java.util.Map;

public class WordCountTopologMain {
    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout",new MySpout(),2);
        topologyBuilder.setBolt("mybolt1", new MySplitBolt1(), 10).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("mybolt2", new MySplitBolt2(), 2).fieldsGrouping("mybolt1", new Fields("word"));
        //创建一个configuration，用来指定当前  topology需要worker的数量
        Config config = new Config();
        config.setNumWorkers(2);
        //  StormSubmitter.submitTopology("mywordCount",config,topologyBuilder.createTopology());
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("mywordCount", config, topologyBuilder.createTopology());
    }


}

