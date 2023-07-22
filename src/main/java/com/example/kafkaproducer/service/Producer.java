package com.example.kafkaproducer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {

    private KafkaTemplate<String, Object> kafkaTemplate;
    String topicName = "testTopic1";

    @Autowired
    public Producer(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void pub(String msg) {
        kafkaTemplate.send(topicName, msg);
    }

    public void sendJoinedMsg(String topicNm, Object msg) {
        kafkaTemplate.send(topicNm, msg);
    }

}
