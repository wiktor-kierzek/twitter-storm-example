package com.meetup.ml.storm.bolt.reporting;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.List;

/**
 * Created by _sn on 29.05.2016.
 */
@Slf4j
public class PrinterBolt extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        switch (tuple.getSourceComponent()) {
            case "stats-tweets-windowed":
                log.info("Current stats: \n{}",
                    Arrays.toString(((List<String>) tuple.getValueByField("stats")).toArray())
                );
                break;
            case "filter-hashtags":
                log.info("New tweet from {} with monitored hashtag: {}", tuple.getStringByField("user_name"), tuple.getStringByField("text"));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
