package com.hello;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class SimpleTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException,InterruptedException{
		
		TopologyBuilder tb = new TopologyBuilder();
		
		CountRedis.createJedis(args[1]);
		tb.setSpout("stringGenerator", new StringGenerator(), 6);
		tb.setBolt("countbolt", new CountRedis(),12).shuffleGrouping("stringGenerator");
		
		Config c = new Config();
		c.setDebug(true);
		
		if (args != null && args.length > 0) {
		    c.setNumWorkers(4);

		    StormSubmitter.submitTopologyWithProgressBar(args[0], c, tb.createTopology());
		}
		else {
		    c.setMaxTaskParallelism(3);

		    LocalCluster cluster = new LocalCluster();
		    cluster.submitTopology("simple-topology", c, tb.createTopology());

		    Thread.sleep(10000);

		    cluster.shutdown();
		}
	}
}
