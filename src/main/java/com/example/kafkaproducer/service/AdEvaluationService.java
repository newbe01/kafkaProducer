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
                .toTable(Materialized.<String, WatchingAdLog, KeyValueStore<Bytes, byte[] >>as("adStore")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(watchingSerde)
                );

        KStream<String, PurchaseLog> purchaseLogKStream =
                sb.stream("OrderLog", Consumed.with(Serdes.String(), purchaseSerde))
                        .filter((key, value) -> value.getPrice() > 1000000);

        purchaseLogKStream.foreach((k,v) -> {
            for (String prodId : v.getProductId()) {

                if (v.getPrice() < 1000000) {
                    PurchaseLogOneProduct tmpVo = new PurchaseLogOneProduct();
                    tmpVo.setUserId(v.getUserId());
                    tmpVo.setProductId(prodId);
                    tmpVo.setOrderId(v.getOrderId());
                    tmpVo.setPrice(v.getPrice());
                    tmpVo.setPurchasedDt(v.getPurchasedDt());

                    producer.sendJoinedMsg("oneProduct", tmpVo);
                }
            }
        });

        KStream<String, PurchaseLogOneProduct> oneStream
                = sb.stream("oneProduct", Consumed.with(Serdes.String(), purchaseOneSerde));


        ValueJoiner<WatchingAdLog, PurchaseLog, EffectOrNot> tableStreamJoiner = (leftValue, rightValue) -> {
            EffectOrNot returnValue = new EffectOrNot();

                returnValue.setUserId(leftValue.getUserId());
                returnValue.setAdId(leftValue.getAdId());
                returnValue.setEffectiveness("Y");

            return returnValue;
        };

    }

}
