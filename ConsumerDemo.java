package de.firsdata.ipg;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerDemo {

	public static void main(){
		 // Create  consumer config properties
		Properties consumerProperties = new Properties();
		final String bootstrapServers = "127.0.0.1:9092";
		final String groupId = "ipg-notification-group";
		final String topicId = "ipg_notification_log";
		consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);
		
		//subscribe to consumer
		//consumer.subscribe(Collections.singleton(topicId));
		
		consumer.subscribe(Arrays.asList(topicId));
		
		while(true){
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, String> record: consumerRecords){
				System.out.println(record.key() + " => " + record.value());	
			}
			
		}
	
	}
}
