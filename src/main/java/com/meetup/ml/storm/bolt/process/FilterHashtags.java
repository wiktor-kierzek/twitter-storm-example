package com.meetup.ml.storm.bolt.process;

import com.meetup.ml.storm.bolt.TweetBolt;
import lombok.AllArgsConstructor;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Tuple;

import java.util.List;

/**
 * Created by _sn on 29.05.2016.
 */
@AllArgsConstructor
public class FilterHashtags extends TweetBolt {

    private List<String> hashtags;


    public void execute(Tuple tuple, BasicOutputCollector collector) {
        List<String> tweeetHashtags = (List<String>) tuple.getValueByField("hashtags");

        for(String hash: hashtags) {
            if(tweeetHashtags.contains(hash)) {
                collector.emit(tuple.getValues());
            }
        }
    }

}
