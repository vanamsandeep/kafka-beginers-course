package org.kafka.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

    public static void main(String[] args) {

        log.info("Hello world");

        //create producer properties
        Properties properties = new Properties();
        //connect to upstash cluster

        properties.put("bootstrap.servers", "https://star-cod-6025-us1-kafka.upstash.io:9092");
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"c3Rhci1jb2QtNjAyNSTIozOHWX93TL34MguD9YkBHy1fVCdMnLsPVaHutBPTFUA\" password=\"ZGJiOWNmMDAtZGFhMC00MThiLTg4NTgtMGE4N2JmNmY1YjU1\";");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world testing the  testing message");

        //send data
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    log.info("Received Meta Data \n" +
                            "Topic :"+recordMetadata.topic() +"\n"+
                            "Partition :"+ recordMetadata.partition()+"\n"+
                            "Offset: "+ recordMetadata.offset() +" \n" +
                            "time stamp"+ recordMetadata.timestamp() +"\n"+
                            "to string "+ recordMetadata);
                }else{
                    log.error("Error happend while creating messages " +e);
                }
            }
        });

        //flush out the
        producer.flush();

        //tell producer to send all data and block until done--->synchronous operation
        producer.close();


    }
}
