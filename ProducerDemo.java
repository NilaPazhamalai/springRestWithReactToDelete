package de.firsdata.ipg;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
       // Create producer properties
		Properties producerProperties = new Properties();
		final String bootstrapServers = "127.0.0.1:9092";
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	
	KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);
	ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("ipg_notification_log", "record-1");
	
	
	
	
	
	
	
	
	
	ProducerRecord<String, String> producerRecord2 = new ProducerRecord<String, String>("ipg-notification-log", "record-2");
	ProducerRecord<String, String> producerRecord3 = new ProducerRecord<String, String>("ipg-notification-log", "record-3");
	
	
	
	producer.send(producerRecord);
	
	// flush and cose
	producer.flush();
	producer.close();
	}

}
