package com.meetup.ml.storm.bolt.process;

import com.meetup.ml.storm.bolt.TweetBolt;
import lombok.AllArgsConstructor;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.List;

/**
 * Created by _sn on 29.05.2016.
 */
@AllArgsConstructor
public class FilterBolt extends TweetBolt {

    private String fieldName;
    private List<Object> acceptedValues;

    public FilterBolt(String fieldName, Object[] acceptedValues) {
        this.fieldName = fieldName;
        this.acceptedValues = Arrays.asList(acceptedValues);
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {

        Object fieldValue = tuple.getValueByField(fieldName);

        if(acceptedValues.contains(fieldValue)) {
            collector.emit(tuple.getValues());
        }
    }

}
