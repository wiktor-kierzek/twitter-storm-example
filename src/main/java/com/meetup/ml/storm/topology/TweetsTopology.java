package com.meetup.ml.storm.topology;

import com.meetup.ml.storm.bolt.process.FilterBolt;
import com.meetup.ml.storm.bolt.process.FilterHashtags;
import com.meetup.ml.storm.bolt.process.HashtagStats;
import com.meetup.ml.storm.bolt.process.TweetParser;
import com.meetup.ml.storm.bolt.reporting.PrinterBolt;
import com.meetup.ml.storm.spout.TweetsSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Created by _sn on 29.05.2016.
 */
public class TweetsTopology {
    public static StormTopology getInstance() {
        TopologyBuilder builder = new TopologyBuilder();

        TweetsSpout tweetsSpout = new TweetsSpout(new String[] {"meetup", "java", "python"});
        builder.setSpout("input-tweets", tweetsSpout, 1);

        TweetParser parser = new TweetParser();
        builder.setBolt("parser-tweets", parser).shuffleGrouping("input-tweets");

        builder.setBolt("stats-tweets-windowed", new HashtagStats()).shuffleGrouping("parser-tweets");

        FilterBolt fiterUsers = new FilterBolt("user_id", new Long[] {28840870L, 736936425409683456L});
        builder.setBolt("filter-users", fiterUsers).shuffleGrouping("parser-tweets");

        FilterHashtags filterHashtags = new FilterHashtags(new String[] {"test", "hahstag"});
        builder.setBolt("filter-hashtags", filterHashtags).shuffleGrouping("filter-users");

        PrinterBolt printerBolt = new PrinterBolt();
        builder.setBolt("printer", printerBolt)
            .shuffleGrouping("filter-hashtags")
            .shuffleGrouping("stats-tweets-windowed");

        return builder.createTopology();
    }
}
