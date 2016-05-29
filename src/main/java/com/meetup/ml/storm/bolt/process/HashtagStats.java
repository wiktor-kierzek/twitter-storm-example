package com.meetup.ml.storm.bolt.process;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by _sn on 29.05.2016.
 */
public class HashtagStats extends BaseWindowedBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(TupleWindow tupleWindow) {

        Map<String, Integer> stats = new HashMap<String, Integer>();

        for(Tuple tuple: tupleWindow.get()) {
            for(String hashtag: (List<String>)tuple.getValueByField("hashtags")) {
                if(stats.containsKey(hashtag)) {
                    stats.put(hashtag, stats.get(hashtag)+1);
                } else {
                    stats.put(hashtag, 1);
                }
            }
        }

        Comparator<Map.Entry<String, Integer>> valueComparator =
            (e1, e2) -> e1.getValue().compareTo(e2.getValue()) * -1;

        Map<String, Integer> sortedMap = stats.entrySet().stream()
            .sorted(valueComparator)
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,(e1, e2) -> e1,
                LinkedHashMap::new)
            );

        List<String> results = new LinkedList<>();
        for(Map.Entry<String, Integer> entry: sortedMap.entrySet()) {
            if(results.size()>5) break;
            results.add(String.format("%s(%d)", entry.getKey(), entry.getValue()));
        }

        collector.emit(new Values(results));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stats"));
    }

}
