package com.example.kafkaproducer.service;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
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
                Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, String> rightStream = streamsBuilder.stream(
                "rightTopic",
                Consumed.with(STRING_SERDE, STRING_SERDE));

        ValueJoiner<String, String, String> stringJoiner = (leftValue, rightValue) -> {
            return "[StringJoiner]" + leftValue + "-" + rightValue;
        };

        ValueJoiner<String, String, String> stringOuterJoiner = (leftValue, rightValue) -> {
            return "[StringOuterJoiner]" + leftValue + "<" + rightValue;
        };

        KStream<String, String> joinStream = leftStream.join(rightStream,
                stringJoiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1))
        );

        KStream<String, String> outerJoinedStream = leftStream.outerJoin(rightStream,
                stringOuterJoiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1))
        );

     joinStream.print(Printed.toSysOut());
     joinStream.to("joinedMsg");
     outerJoinedStream.print(Printed.toSysOut());
     outerJoinedStream.to("joinedMsg");

    }

}
