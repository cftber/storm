package stormdemo.stormtest.WorldCounter;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * Created by tgzhao on 2016/5/10.
 */
public class WordReaderSpout extends BaseRichSpout {
    private FileReader fileReader;
    private SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            fileReader = new FileReader(new File("D:\\words.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        String str;
        BufferedReader bf = new BufferedReader(fileReader);
        try {
            while ((str = bf.readLine()) != null) {
                collector.emit(new Values(str));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
