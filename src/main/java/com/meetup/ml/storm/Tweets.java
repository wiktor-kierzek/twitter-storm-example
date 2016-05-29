package com.meetup.ml.storm;

import com.meetup.ml.storm.topology.TweetsTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.utils.Utils;

/**
 * Created by _sn on 29.05.2016.
 */
public class Tweets {
    public static void main(String[] args) {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("tweets", conf, TweetsTopology.getInstance());
        Utils.sleep(3600000);
        cluster.killTopology("tweets");
        cluster.shutdown();

    }
}
