package com.example.kafkaproducer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

//@EnableKafka
@EnableKafkaStreams
@Configuration
public class KafkaConfig {

//    @Bean
//    public KafkaTemplate<String, Object> KafkaTemplate() {
//        return new KafkaTemplate<String, Object>(ProducerFactory());
//    }
//
//    @Bean
//    public ProducerFactory<String, Object> ProducerFactory() {
//
//        Map<String, Object> config = new HashMap<>();
//        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "43.201.55.0:9092");
//        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//
//        return new DefaultKafkaProducerFactory<>(config);
//    }
//
//    @Bean
//    public ConsumerFactory<String, Object> ConsumerFactory() {
//
//        Map<String, Object> config = new HashMap<>();
//        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "43.201.55.0:9092");
//        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//
//        return new DefaultKafkaConsumerFactory<>(config);
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(ConsumerFactory());
//
//        return factory;
//    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamConfig(){
        Map<String, Object> streamConfig = new HashMap<>();
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "testTopic");
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "3.38.179.14:9092");
        streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

        return new KafkaStreamsConfiguration(streamConfig);
    }

}
