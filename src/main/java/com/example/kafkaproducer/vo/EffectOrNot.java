package com.example.kafkaproducer.vo;

import lombok.Data;

import java.util.Map;

@Data
public class EffectOrNot {

    String adId;
    String userId;
    String  orderId;
    Map<String, String> productInfo;

}
