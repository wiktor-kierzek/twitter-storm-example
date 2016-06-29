package com.meetup.ml.storm.spout;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by _sn on 29.05.2016.
 */
@Slf4j
public class TweetsSpout extends BaseRichSpout {

    private static String ACCESS_TOKEN = "28840870-LHgFC5LjVhPtLwJzq2UrMQVB49uWqBeGq8sTOgzhU";
    private static String ACCESS_TOKEN_SECRET = "JO0y6SXRSM5snRNRyEXrefr7FhoIGWXhk982zvf7gODGs";
    private static String CONSUMER_KEY = "YqVB1bjECOCShoy8ecl942G8K";
    private static String CONSUMER_SECRET = "eWHNiB6r9LOtLvICBxxFHKiFA3yy2p4stusFzZAeMRo2l9CyzR";

    private List<String> trackedTerms;
    private List<Long> trackedUsers;

    private BlockingQueue<String> tweets = new LinkedBlockingQueue<String>(1000);
    private SpoutOutputCollector collector;

    public TweetsSpout(String[] trackedTerms) {
        this.trackedTerms = Arrays.asList(trackedTerms);
        this.trackedUsers = null;
    }

    public TweetsSpout(String[] trackedTerms, Long[] trackedUsers) {
        this.trackedTerms = Arrays.asList(trackedTerms);
        this.trackedUsers = Arrays.asList(trackedUsers);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;

        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint filter = new StatusesFilterEndpoint();

        if(trackedTerms!=null) {
            filter.trackTerms(trackedTerms);
        }

        if(trackedUsers!=null) {
            filter.followings(trackedUsers);
        }

        Authentication auth = new OAuth1(
            CONSUMER_KEY,
            CONSUMER_SECRET,
            ACCESS_TOKEN,
            ACCESS_TOKEN_SECRET
        );

        Client client = new ClientBuilder()
            .name("storm-example")
            .hosts(hosts)
            .authentication(auth)
            .endpoint(filter)
            .processor(new StringDelimitedProcessor(tweets))
            .build();

        client.connect();
    }

    public void deactivate() {
    }

    public void nextTuple() {
        if(tweets.isEmpty()) {
            return;
        }

        try {
            String message = tweets.take();
            collector.emit(new Values(message));

        } catch (InterruptedException e) {
            log.error("Could not get message from queue", e);
        }
    }

}
