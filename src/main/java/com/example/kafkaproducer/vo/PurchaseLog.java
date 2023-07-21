package com.example.kafkaproducer.vo;

import lombok.Data;

import java.util.ArrayList;

@Data
public class PurchaseLog {

    String orderId;
    String userId;
    ArrayList<String> productId;
    String purchasedDt;
    Long price;

}
