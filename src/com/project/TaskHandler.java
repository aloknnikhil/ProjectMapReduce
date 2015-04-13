package com.project;

import com.project.utils.LogFile;
import com.project.utils.Node;
import com.project.utils.Task;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.List;
import java.util.Properties;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class TaskHandler {

    private static TaskHandler resourceManagerInstance;
    private Producer<String, Task> taskProducer;

    private TaskHandler() {

        Properties properties = new Properties();

        properties.put("metadata.broker.list", MapRSession.getInstance()
                .getZookeeperHost() + ":9092," + MapRSession.getInstance().getZookeeperHost() +  ":9092");
        properties.put("serializer.class", "com.project.utils.Task");
        properties.put("partitioner.class", "example.producer.SimplePartitioner");
        properties.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(properties);
        taskProducer = new Producer<String, Task>(config);

        LogFile.writeToLog("Connected to Zookeeper Configuration Manager");
    }

    public static void configureResourceManager(List<Integer> slaveIDs) {

    }

    public static void dispatchTask(Task task) {
        String taskExecutor = task.getExecutorID() + "";
        KeyedMessage<String, Task> taskKeyedMessage = new KeyedMessage<String, Task>(taskExecutor,
                task.getTaskID() + "", task);
        getInstance().taskProducer.send(taskKeyedMessage);
    }

    public static void modifyTask(Task task) {
    }

    public static Task getActiveTaskFor(String nodePath) {
        return new Task();
    }

    public static TaskHandler getInstance() {
        if (resourceManagerInstance == null) {
            resourceManagerInstance = new TaskHandler();
        }
        return resourceManagerInstance;
    }

    public static void setWatcherOn(String path, Node node) {
    }
}
