package com.xiaoxiao.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class LogConsumer {

	private ConsumerConfig config;
	private String topic;
	private int partitionsNum;
	private MessageExecutor executor;
	private ConsumerConnector connector;
	private ExecutorService threadPool;

	public LogConsumer(String topic, int partitionsNum, MessageExecutor executor)
			throws Exception {
		Properties properties = new Properties();
		properties.load(ClassLoader
				.getSystemResourceAsStream("consumer.properties"));
		config = new ConsumerConfig(properties);
		this.topic = topic;
		this.partitionsNum = partitionsNum;
		this.executor = executor;
	}

	public void start() throws Exception {
		connector = Consumer.createJavaConsumerConnector(config);
		Map<String, Integer> topics = new HashMap<String, Integer>();
		topics.put(topic, partitionsNum);
		Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector
				.createMessageStreams(topics);
		List<KafkaStream<byte[], byte[]>> partitions = streams.get(topic);
		threadPool = Executors.newFixedThreadPool(partitionsNum);
		for (KafkaStream<byte[], byte[]> partition : partitions) {
			threadPool.execute(new MessageRunner(partition));
		}
	}

	public void close() {
		try {
			threadPool.shutdownNow();
		} catch (Exception e) {
			//
		} finally {
			connector.shutdown();
		}

	}

	class MessageRunner implements Runnable {
		private KafkaStream<byte[], byte[]> partition;

		MessageRunner(KafkaStream<byte[], byte[]> partition) {
			this.partition = partition;
		}

		public void run() {
			ConsumerIterator<byte[], byte[]> it = partition.iterator();
			while (it.hasNext()) {
				MessageAndMetadata<byte[], byte[]> item = it.next();
				executor.execute(new String(item.message()));// UTF-8
			}
		}
	}

	interface MessageExecutor {

		public void execute(String message);
	}

	private static class Record {
		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public long getTimeCost() {
			return timeCost;
		}

		public void setTimeCost(long timeCost) {
			this.timeCost = timeCost;
		}

		private int id;
		private long timeCost;

		public Record(int id, long timeCost) {
			this.id = id;
			this.timeCost = timeCost;
		}

	}

	private final static BlockingQueue<Record> queue = new ArrayBlockingQueue<Record>(
			10000);
	
	
	private static class Printer extends Thread{

		@Override
		public void run() {
			while(true){
				long mx = 0;
				Record record = null;
				int count=0;
				while ((record = queue.poll()) != null) {
					mx = Math.max(record.getTimeCost(), mx);
					count++;
				}
				System.out.println(count+":"+mx/1000000.0);
				try {
					this.sleep(1000);
				} catch (InterruptedException e) {
					break;
				}
			}
			
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		LogConsumer consumer = null;
		try {
			MessageExecutor executor = new MessageExecutor() {

				public void execute(String message) {
					String args[] = message.split("=");
					try{
						int id = Integer.parseInt(args[0]);
						long timeStamp = Long.parseLong(args[1]);
						queue.add(new Record(id, System.nanoTime() - timeStamp));
					}
					catch(Exception e){
						
					}
				}
			};
			consumer = new LogConsumer("topic", 2, executor);
			consumer.start();
			new Printer().start();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// if(consumer != null){
			// consumer.close();
			// }
		}

	}

}