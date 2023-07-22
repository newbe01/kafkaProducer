package com.example.kafkaproducer.service;

import com.example.kafkaproducer.vo.EffectOrNot;
import com.example.kafkaproducer.vo.PurchaseLog;
import com.example.kafkaproducer.vo.PurchaseLogOneProduct;
import com.example.kafkaproducer.vo.WatchingAdLog;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class AdEvaluationService {

    @Autowired
    Producer producer;

    @Autowired
    public void buildPipeline(StreamsBuilder sb) {

        JsonSerializer<EffectOrNot> effectSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLog> purchaseSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLogOneProduct> purchaseOneSerializer = new JsonSerializer<>();
        JsonSerializer<WatchingAdLog> watchingSerializer = new JsonSerializer<>();


        JsonDeserializer<EffectOrNot> effectDeserializer = new JsonDeserializer<>(EffectOrNot.class);
        JsonDeserializer<PurchaseLog> purchaseDeserializer = new JsonDeserializer<>(PurchaseLog.class);
        JsonDeserializer<PurchaseLogOneProduct> purchaseOneDeserializer = new JsonDeserializer<>(PurchaseLogOneProduct.class);
        JsonDeserializer<WatchingAdLog> watchingDeserializer = new JsonDeserializer<>(WatchingAdLog.class);

        Serde<EffectOrNot> effectSerde = Serdes.serdeFrom(effectSerializer, effectDeserializer);
        Serde<PurchaseLog> purchaseSerde = Serdes.serdeFrom(purchaseSerializer, purchaseDeserializer);
        Serde<PurchaseLogOneProduct> purchaseOneSerde = Serdes.serdeFrom(purchaseOneSerializer, purchaseOneDeserializer);
        Serde<WatchingAdLog> watchingSerde = Serdes.serdeFrom(watchingSerializer, watchingDeserializer);

        KTable<String, WatchingAdLog> adTable = sb.stream("AdLog", Consumed.with(Serdes.String(), watchingSerde))
                .selectKey((k, v) -> v.getUserId() + "_" + v.getProductId())
                .toTable(Materialized.<String, WatchingAdLog, KeyValueStore<Bytes, byte[]>>as("adStore")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(watchingSerde)
                );

        KStream<String, PurchaseLog> purchaseLogKStream =
                sb.stream("OrderLog", Consumed.with(Serdes.String(), purchaseSerde));

        purchaseLogKStream.foreach((k,v) -> {
            for (Map<String, String> prodInfo : v.getProductInfo()) {

                if (Integer.valueOf(prodInfo.get("price")) < 1000000) {
                    PurchaseLogOneProduct tmpVo = new PurchaseLogOneProduct();
                    tmpVo.setUserId(v.getUserId());
                    tmpVo.setProductId(prodInfo.get("productId"));
                    tmpVo.setOrderId(v.getOrderId());
                    tmpVo.setPrice(prodInfo.get("price"));
                    tmpVo.setPurchasedDt(v.getPurchasedDt());

                    producer.sendJoinedMsg("oneProduct", tmpVo);
                }
            }
        });

        KTable<String, PurchaseLogOneProduct> oneTable
                = sb.stream("oneProduct", Consumed.with(Serdes.String(), purchaseOneSerde))
                .selectKey((k, v) -> v.getUserId() + "_" + v.getProductId())
                .toTable(Materialized.<String, PurchaseLogOneProduct, KeyValueStore<Bytes, byte[]>>as("purchaseLogStore")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(purchaseOneSerde));


        ValueJoiner<WatchingAdLog, PurchaseLogOneProduct, EffectOrNot> tableStreamJoiner = (leftValue, rightValue) -> {
            EffectOrNot returnValue = new EffectOrNot();

                returnValue.setUserId(rightValue.getUserId());
                returnValue.setAdId(leftValue.getAdId());
                returnValue.setOrderId(rightValue.getOrderId());
            Map<String, String> tempProdInfo = new HashMap<>();
            tempProdInfo.put("productId", rightValue.getProductId());
            tempProdInfo.put("price", rightValue.getPrice());
            returnValue.setProductInfo(tempProdInfo);

            return returnValue;
        };

        adTable.join(oneTable, tableStreamJoiner)
                .toStream().to("AdEvaluationComplete", Produced.with(Serdes.String(), effectSerde));

    }

}
