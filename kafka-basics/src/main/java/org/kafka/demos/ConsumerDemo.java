package org.kafka.demos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        log.info("--------This is consumer-----");
        String groupId = "my-java-application";
        String topic = "demo_java";

        //create producer properties
        Properties properties = new Properties();
        //connect to upstash cluster

        properties.put("bootstrap.servers", "https://star-cod-6025-us1-kafka.upstash.io:9092");
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"c3Rhci1jb2QtNjAyNSTIozOHWX93TL34MguD9YkBHy1fVCdMnLsPVaHutBPTFUA\" password=\"ZGJiOWNmMDAtZGFhMC00MThiLTg4NTgtMGE4N2JmNmY1YjU1\";");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        //set consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");


        //create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info(" Detected a shutdown, let's exit by calling consumer.wakeup().....");
                consumer.wakeup();
                // join the main thread to allow the execution of the code in the main thread
                try{
                    mainThread.join();
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        try {

            //subscribe to topic
            consumer.subscribe(Arrays.asList(topic));
            while(true){
                log.info("polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record: records){
                    log.info("key :"+record.key() +" value : "+record.value());
                    log.info("partition :"+record.partition() +" offset : "+record.offset());
                }
            }

        }catch (WakeupException e) {
            log.info("Consumer is starting to shut down", e);
        }catch(Exception e){
            log.info("Un expected exception happened ", e);
        }finally {
            consumer.close(); // close the consumer, this will also commit offsets
            log.info("The consumer is now gracefully shut down ");
        }

    }
}
