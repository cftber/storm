package stormdemo.stormtest;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SimpleBolt extends BaseRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		System.out.println("======Bolt=====prepare========");
		
	}

	public void execute(Tuple input) {
		String desc = "out :"+ input.getString(0);
		System.out.println("====Bolt========desc:"+desc);
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println("====Bolt==declarer========");
		
	}

}
