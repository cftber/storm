package stormdemo.stormtest.WorldCounter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tgzhao on 2016/5/10.
 */
public class WordReaderCountBolt extends BaseRichBolt {
    Map<String, Integer> counters;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        System.out.println("======Bolt=====prepare========");
        this.counters = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple) {
        String str = tuple.getString(0);
        /**
         * If the word dosn't exist in the map we will create
         * this, if not We will add 1
         */
        if(!counters.containsKey(str)){
            counters.put(str, 1);
        }else{
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        System.out.println(counters.values());
        System.out.println("====Bolt========desc:");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        System.out.println("====Bolt==declarer========");
    }
}
