package com.project;

import com.project.utils.Task;
import com.project.utils.TaskChangeListener;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class TaskHandler {

    private static TaskHandler resourceManagerInstance;
    private Producer<String, byte[]> taskProducer;
    private ConsumerConnector consumer;

    private TaskHandler() {

        Properties producerProperties = new Properties();

        producerProperties.put("metadata.broker.list", MapRSession.getInstance()
                .getKafkaHost() + ":9092," + MapRSession.getInstance().getKafkaHost() + ":9092");
        producerProperties.put("serializer.class", "kafka.serializer.DefaultEncoder");
        producerProperties.put("key.serializer.class", "kafka.serializer.StringEncoder");
        producerProperties.put("request.required.acks", "1");

        ProducerConfig producerConfig = new ProducerConfig(producerProperties);
        taskProducer = new Producer<>(producerConfig);

        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", MapRSession.getInstance().getZookeeperHost());
        consumerProperties.put("group.id", "task_group");

        ConsumerConfig consumerConfig = new ConsumerConfig(consumerProperties);
        consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    public void subscribeToSlaves() {
        for(String taskTopic : ResourceManager.getIdleSlavePaths()) {
            subscribeTo(taskTopic, MapRSession.getInstance().getActiveNode());
        }
    }

    public static void dispatchTask(Task task) {
        String taskExecutor = "" + task.getCurrentExecutorID();
        KeyedMessage<String, byte[]> taskKeyedMessage = new KeyedMessage<>(taskExecutor,
                task.getTaskID() + "", Task.serialize(task));
        getInstance().taskProducer.send(taskKeyedMessage);
    }

    public static void modifyTask(Task task) {
        dispatchTask(task);
    }

    public static TaskHandler getInstance() {
        if (resourceManagerInstance == null) {
            resourceManagerInstance = new TaskHandler();
        }
        return resourceManagerInstance;
    }

    public static void subscribeTo(String path, TaskChangeListener callback) {

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(path, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = getInstance().consumer
                .createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(path);

        for (final KafkaStream stream : streams) {
            new Thread(new TaskSubscriber(stream, callback)).start();
        }
    }

    private static class TaskSubscriber implements Runnable {

        TaskChangeListener taskChangeListener;
        private KafkaStream kafkaStream;

        private TaskSubscriber(KafkaStream kafkaStream, TaskChangeListener taskChangeListener)   {
            this.taskChangeListener = taskChangeListener;
            this.kafkaStream = kafkaStream;
        }

        @Override
        public void run() {
            final ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
            while (iterator.hasNext()) {
                new Thread(new Runnable() {
                    byte[] bytes = iterator.next().message();
                    @Override
                    public void run() {
                        taskChangeListener.onTaskChanged(Task.deserialize(bytes));
                    }
                }).start();
            }
        }
    }
}
