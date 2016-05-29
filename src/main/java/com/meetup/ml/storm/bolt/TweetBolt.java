package com.meetup.ml.storm.bolt;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;

/**
 * Created by _sn on 29.05.2016.
 */
public abstract class TweetBolt extends BaseBasicBolt {

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "user_id", "user_name", "text", "timestamp", "hashtags", "urls", "language"));
    }
}
