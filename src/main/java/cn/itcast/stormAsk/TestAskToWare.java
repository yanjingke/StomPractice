package cn.itcast.stormAsk;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


public class TestAskToWare {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("mySpout",new MySpout(),1);
        topologyBuilder.setBolt("myBolt1",new MyBolt1(),1).shuffleGrouping("mySpout");
        Config config=new Config();
        LocalCluster cluster=new LocalCluster();
        cluster.submitTopology("TestAskToWare",config,topologyBuilder.createTopology());
    }

}
