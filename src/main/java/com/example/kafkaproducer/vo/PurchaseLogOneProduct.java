package com.example.kafkaproducer.vo;

import lombok.Data;

@Data
public class PurchaseLogOneProduct {

    String orderId;
    String userId;
    String productId;
    String purchasedDt;
    String price;
}
