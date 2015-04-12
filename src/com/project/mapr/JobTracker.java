package com.project.mapr;

import com.project.ResourceManager;
import com.project.storage.FileSystem;
import com.project.utils.Input;
import com.project.utils.Node;
import com.project.utils.Output;
import com.project.utils.Task;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.*;
import java.util.*;

/**
 * Created by alok on 4/11/15.
 */
public class JobTracker {

    private HashMap<Integer, Queue<Task>> runningTasks;
    private HashMap<Integer, Queue<Task>> completedTasks;
    private List<Task> pendingTasks;
    private HashMap<Integer, Queue<Task>> allTasks;
    private List<Node> activeSlaves;
    private Input jobInput;
    private Output jobOutput;
    public Watcher taskWatcher;

    private File intermediateDir = new File("intermediate");

    public JobTracker(Input inputFile) {
        runningTasks = new HashMap<>();
        completedTasks = new HashMap<>();
        allTasks = new HashMap<>();
        pendingTasks = new ArrayList<>();
        taskWatcher = new TaskWatcher();
        jobInput = inputFile;
        jobOutput = new Output(new File("results.txt"));
        if (!intermediateDir.exists())
            intermediateDir.mkdir();
    }

    public void start() {
        connectToSlaves();
        initializeMapTasks();
        assignTasks();
        beginTasks();
    }

    private void connectToSlaves()  {
        List<Node> slaveNodes = new ArrayList<>();
        while(ResourceManager.getIdleSlavePaths().size() != ResourceManager.getAllSlavePaths().size())  {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (String slavePath : ResourceManager.getIdleSlavePaths())    {
            slaveNodes.add(ResourceManager.getNodeFrom(slavePath));
        }
        this.activeSlaves = slaveNodes;
    }

    private void initializeMapTasks() {
        Task task;
        Input taskInput;
        int count = 1;
        pendingTasks.clear();

        for (File file : jobInput.getLocalFile().listFiles()) {
            task = new Task();
            task.setType(Task.Type.MAP);
            task.setStatus(Task.Status.INITIALIZED);
            task.setTaskID(count);
            taskInput = new Input(FileSystem.copyFromLocalFile(file));
            task.setTaskInput(taskInput);
            pendingTasks.add(task);
            count++;
        }
    }

    private void initializeReduceTasks() {
        Task task;
        Input taskInput;
        int count = 1;
        for (File file : intermediateDir.listFiles()) {
            task = new Task();
            task.setType(Task.Type.REDUCE);
            task.setStatus(Task.Status.INITIALIZED);
            task.setTaskID(count);
            taskInput = new Input(FileSystem.copyFromLocalFile(file));
            task.setTaskInput(taskInput);
            pendingTasks.add(task);
            count++;
        }
    }

    private void assignTasks() {
        Iterator nodeIterator = activeSlaves.iterator();
        Node temp;
        for (Task task : pendingTasks) {
            pendingTasks.remove(task);
            if (!nodeIterator.hasNext())
                nodeIterator = activeSlaves.iterator();

            temp = (Node) nodeIterator.next();

            if (allTasks.containsKey(temp.getNodeID())) {
                task.setExecutorID(temp.getNodeID());
                allTasks.get(temp.getNodeID()).add(task);
            }
        }
    }

    private void beginTasks() {
        for (Map.Entry<Integer, Queue<Task>> entry : allTasks.entrySet()) {
            ResourceManager.dispatchTask(entry.getValue().peek());
        }
    }

    private void markTaskComplete(Task task) {
        if (completedTasks.containsKey(task.getExecutorID())) {
            completedTasks.get(task.getExecutorID()).add(task);
            runningTasks.get(task.getExecutorID()).remove();
            runningTasks.put(task.getExecutorID(), runningTasks.get(task.getExecutorID()));
        }
    }

    public Task getPendingTaskFor(Node slaveNode) {
        return new Task();
    }

    public HashMap<Integer, Queue<Task>> getRunningTasks() {
        return runningTasks;
    }

    public HashMap<Integer, Queue<Task>> getCompletedTasks() {
        return completedTasks;
    }

    public HashMap<Integer, Queue<Task>> getAllTasks() {
        return allTasks;
    }

    public List<Task> getPendingTasks() {
        return pendingTasks;
    }

    public class TaskWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {

            switch (event.getType()) {
                case NodeDataChanged:
                    Task task = ResourceManager.getActiveTaskFor(event.getPath());

                    switch (task.getStatus()) {
                        case RUNNING:
                            //TODO Check if the task running time exceeded the timeout period
                            //If so, re-schedule on another node
                            break;

                        case COMPLETE:
                            collectTaskOutput(task);
                            markTaskComplete(task);
                    }

                    break;
            }

        }
    }

    private void collectTaskOutput(Task task) {
        File localFile;
        localFile = FileSystem.copyFromRemotePath(task.getTaskOutput().getRemoteDataPath());
        parseKeyValuePair(localFile, task.getType());
    }

    private void parseKeyValuePair(File intermediateFile, Task.Type type) {
        BufferedReader bufferedReader;
        StringTokenizer stringTokenizer;
        String temp, key, value;
        PrintWriter printWriter;

        try {
            bufferedReader = new BufferedReader(new FileReader(intermediateFile));
            while ((temp = bufferedReader.readLine()) != null) {
                stringTokenizer = new StringTokenizer(temp, ":");
                key = stringTokenizer.nextToken();
                value = stringTokenizer.nextToken();
                if (type == Task.Type.MAP) {
                    printWriter = new PrintWriter(new FileWriter(new File(intermediateDir, key), true));
                    printWriter.println(value);
                } else {
                    printWriter = new PrintWriter(new FileWriter(jobOutput.getLocalFile(), true));
                    printWriter.println(key + " " + value);
                }

                printWriter.flush();
                printWriter.close();
            }
            bufferedReader.close();
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }
}
