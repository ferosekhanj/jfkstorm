package com.hello;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class PrintBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple arg0, BasicOutputCollector arg1) {
		// TODO Auto-generated method stub
		String great = arg0.getStringByField("great");
		System.out.println("Name of a great "+great);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
