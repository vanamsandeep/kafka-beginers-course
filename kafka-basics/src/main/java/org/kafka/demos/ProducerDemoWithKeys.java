 package org.kafka.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

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


        for(int i=0; i<10; i++){

            String topic= "demo_java";
            String key = "id_"+i;
            String value = "hello world _"+i;

            //create a producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        log.info(" Key : "+key +" Partition :"+recordMetadata.partition());
                    }else{
                        log.error("Error happend while creating messages " +e);
                    }
                }
            });
        }

        //flush out the
        producer.flush();

        //tell producer to send all data and block until done--->synchronous operation
        producer.close();


    }
}
