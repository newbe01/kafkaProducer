package com.example.kafkaproducer.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.Map;

@Data
public class PurchaseLog {

    String orderId;
    String userId;
//    ArrayList<String> productId;
    ArrayList<Map<String, String>> productInfo;
    String purchasedDt;
//    Long price;

}
