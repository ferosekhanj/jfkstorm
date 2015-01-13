package com.hello;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class StringGenerator extends BaseRichSpout {

	SpoutOutputCollector myCollector;
	String[] greats = {"linus", "rms","stevejobs","stroustrup","jamesgosling",""};
	
	@Override
	public void nextTuple() {
		
		final Random r = new Random();
		myCollector.emit(new Values(greats[r.nextInt(greats.length-1)]));
		
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		
		myCollector = arg2;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
		arg0.declare(new Fields("great"));
		
	}

}
