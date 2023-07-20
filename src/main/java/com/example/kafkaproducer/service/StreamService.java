package com.example.kafkaproducer.service;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class StreamService {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {

//        KStream<String, String> myStream = streamsBuilder.stream("testTopic", Consumed.with(STRING_SERDE, STRING_SERDE));
//        myStream.print(Printed.toSysOut());
//        myStream.filter((key, value) -> value.contains("test")).to("testList");

        KStream<String, String> leftStream = streamsBuilder.stream(
                "leftTopic",
                Consumed.with(STRING_SERDE, STRING_SERDE)
        ).selectKey((k,v) -> v.substring(0,v.indexOf(":")));

        KStream<String, String> rightStream = streamsBuilder.stream(
                "rightTopic",
                Consumed.with(STRING_SERDE, STRING_SERDE)
        ).selectKey((k,v) -> v.substring(0,v.indexOf(":")));

        leftStream.print(Printed.toSysOut());
        rightStream.print(Printed.toSysOut());

        KStream<String, String> joinStream = leftStream.join(rightStream,
                (leftValue, rightValue) -> leftValue + "_" + rightValue,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1))
        );

     joinStream.print(Printed.toSysOut());
     joinStream.to("joinedMsg");

    }

}
