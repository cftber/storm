package stormdemo.stormtest.WorldCounter;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by tgzhao on 2016/5/10.
 */
public class WordReaderSplitBolt extends BaseRichBolt {
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        System.out.println("======WordReaderSplitBolt=====prepare========");
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String str = tuple.getString(0);
        String[] strings = str.split(" ");
        for (int i=0; i < strings.length; i++) {
            collector.emit(new Values(strings[i]));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
