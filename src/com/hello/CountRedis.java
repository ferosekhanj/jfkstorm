package com.hello;


import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.*;

public class CountRedis extends BaseRichBolt {
	
    static JedisPool pool = new JedisPool(new JedisPoolConfig(),"104.41.146.23");
        
	public static void createJedis(String host) {

	    if( pool == null)
		pool = new JedisPool(new JedisPoolConfig(),host);

	}
	
	@Override
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		try (Jedis jedis = pool.getResource()) {
			String great = arg0.getStringByField("great");
			jedis.incr(great);
			jedis.incr("processed");
		}		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
