package stormdemo.stormtest;

import java.util.Map;
import java.util.Random;

import clojure.reflect.Field;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SimpleSpout extends BaseRichSpout{

	private SpoutOutputCollector collector;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String[] names = new String[]{"JAVA","PHP","IOS","ASP","HADOOP"};
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		System.out.println("======Spout=====open========");
	}

	@Override
	public void nextTuple() {
		Utils.sleep(1000 * 3);
		Random rand = new Random();
		collector.emit(new Values(names[rand.nextInt(names.length)]));
		System.out.println("======Spout=====nextTuple========");
	}
	/**
     * 定义字段id，该id在简单模式下没有用处，但在按照字段分组的模式下有很大的用处。
     * 该declarer变量有很大作用，我们还可以调用declarer.declareStream();来定义stramId，该id可以用来定义更加复杂的流拓扑结构
     */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("msg"));
		System.out.println("======Spout=====declareOutputFields========");
		
	}
 
		
 
}
