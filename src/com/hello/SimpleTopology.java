package com.hello;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class SimpleTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException{
		
		TopologyBuilder tb = new TopologyBuilder();
		
		tb.setSpout("stringGenerator", new StringGenerator(), 2);
		tb.setBolt("countbolt", new CountRedis(),4).shuffleGrouping("stringGenerator");
		
		Config c = new Config();
		c.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("simpleTopolgy", c, tb.createTopology());
		
		try{
			Thread.sleep(10000);
		}catch(InterruptedException ie){
			System.out.println("Stopping the cluster");
		}
		cluster.killTopology("simpleTopology");
		cluster.shutdown();
	}
}
