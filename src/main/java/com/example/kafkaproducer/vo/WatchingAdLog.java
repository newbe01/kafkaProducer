package com.example.kafkaproducer.vo;

import lombok.Data;

@Data
public class WatchingAdLog {

    String userId;
    String productId;
    String adId;
    String adType;
    String watchingTime;

}
