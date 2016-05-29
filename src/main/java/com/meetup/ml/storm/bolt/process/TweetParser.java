package com.meetup.ml.storm.bolt.process;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.meetup.ml.storm.bolt.TweetBolt;
import com.meetup.ml.storm.spout.TweetsSpout;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by _sn on 29.05.2016.
 */
public class TweetParser extends TweetBolt {

    private Gson parser;

    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        parser = new Gson();
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        JsonObject tweet = parser.fromJson(tuple.getStringByField("tweet"), JsonObject.class);

        List<String> hashtags = new LinkedList<String>();
        List<String> urls = new LinkedList<String>();

        for(JsonElement hashtag: tweet.getAsJsonObject("entities").getAsJsonArray("hashtags")) {
            hashtags.add(hashtag.getAsJsonObject().get("text").getAsString());
        }

        for(JsonElement url: tweet.getAsJsonObject("entities").getAsJsonArray("urls")) {
            urls.add(url.getAsJsonObject().get("expanded_url").getAsString());
        }

        collector.emit(new Values(
            tweet.get("id").getAsLong(),
            tweet.getAsJsonObject("user").get("id").getAsLong(),
            tweet.getAsJsonObject("user").get("name").getAsString(),
            tweet.get("text").getAsString(),
            new Date(tweet.get("timestamp_ms").getAsLong()),
            hashtags,
            urls,
            tweet.get("lang").getAsString()
        ));
    }
}
