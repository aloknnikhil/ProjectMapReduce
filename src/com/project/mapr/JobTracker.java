package com.project.mapr;

import com.project.ResourceManager;
import com.project.SocketTaskHandler;
import com.project.storage.FileSystem;
import com.project.utils.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by alok on 4/11/15.
 */
public class JobTracker implements Serializable {

    private HashMap<Integer, Queue<Task>> completedTasks;
    private List<Task> allTasks;
    private HashMap<Integer, Task> currentTaskFor;
    private HashMap<Integer, Queue<Task>> pendingTasks;
    private List<Node> activeSlaves;
    private Input jobInput;
    private Output jobOutput;
    private int outstandingTaskCount = 0;
    private boolean isMapPhase = true;

    private HashMap<String, Integer> reduceKeys = new HashMap<>();

    private File intermediateDir = new File("out/intermediate");

    public JobTracker(Input inputFile) {
        completedTasks = new HashMap<>();
        pendingTasks = new HashMap<>();
        currentTaskFor = new HashMap<>();
        allTasks = new ArrayList<>();
        jobInput = inputFile;
        jobOutput = new Output(new File("results.txt"));
        if (!intermediateDir.exists())
            intermediateDir.mkdir();
    }

    public void start() {
        checkForSlaves();
        SocketTaskHandler.getInstance().connectToSlaves();
        initializeMapTasks();
        System.out.println("Pending Tasks: " + allTasks.size());
        assignTasks();
        beginTasks();
    }

    private void checkForSlaves() {
        List<Node> slaveNodes = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : ResourceManager.slaveAddresses.entrySet()) {
            slaveNodes.add(new Node(Node.Type.SLAVE, entry.getKey()));
        }
        this.activeSlaves = slaveNodes;
    }

    public void initializeMapTasks() {
        Task task;
        Input taskInput;
        int count = 1;
        allTasks.clear();

        for (File file : jobInput.getLocalFile().listFiles()) {
            if (file.isDirectory()) {
                for (File subFile : file.listFiles()) {
                    task = new Task();
                    task.setType(Task.Type.MAP);
                    task.setStatus(Task.Status.INITIALIZED);
                    task.setTaskID(count);
                    taskInput = new Input(subFile);
                    task.addTaskInput(taskInput);
                    allTasks.add(task);
                    count++;
                }
            } else {
                task = new Task();
                task.setType(Task.Type.MAP);
                task.setStatus(Task.Status.INITIALIZED);
                task.setTaskID(count);
                taskInput = new Input(file);
                task.addTaskInput(taskInput);
                allTasks.add(task);
                count++;
            }
        }
    }

    public void initializeReduceTasks() {
        Task task;
        Input taskInput;
        int count = 1;

        outstandingTaskCount = 0;
        allTasks.clear();
        pendingTasks.clear();
        completedTasks.clear();
        for (File file : intermediateDir.listFiles()) {
            if (file.isDirectory()) {
                for (File subFile : file.listFiles()) {
                    task = new Task();
                    task.setType(Task.Type.REDUCE);
                    task.setStatus(Task.Status.INITIALIZED);
                    task.setTaskID(count);
                    taskInput = new Input(subFile);
                    task.addTaskInput(taskInput);
                    allTasks.add(task);
                    count++;
                }
            } else {
                task = new Task();
                task.setType(Task.Type.REDUCE);
                task.setStatus(Task.Status.INITIALIZED);
                task.setTaskID(count);
                taskInput = new Input(file);
                task.addTaskInput(taskInput);
                allTasks.add(task);
                count++;
            }
        }
    }

    public void assignTasks() {
        Iterator nodeIterator = activeSlaves.iterator();
        Node temp = null;
        Queue<Task> taskQueue;
        boolean allSlavesDead = false;
        int count = 0;

        for (Task pendingTask : allTasks) {
            do {
                if (allSlavesDead) {
                    System.out.println("All slaves offline! :o");
                    return;
                }

                if (nodeIterator.hasNext()) {
                    temp = (Node) nodeIterator.next();
                } else {
                    nodeIterator = activeSlaves.iterator();
                    count++;
                }

                if (count == 2) {
                    allSlavesDead = true;
                }
            } while (SocketTaskHandler.getInstance().offlineSlaves.contains(temp.getNodeID()));

            allSlavesDead = false;
            count = 0;

            synchronized (pendingTasks) {
                if (!SocketTaskHandler.getInstance().offlineSlaves.contains(temp.getNodeID())) {
                    pendingTask.setExecutorID(temp.getNodeID());
                    if (pendingTasks.containsKey(temp.getNodeID())) {
                        pendingTasks.get(temp.getNodeID()).add(pendingTask);
                    } else {
                        taskQueue = new LinkedBlockingQueue<>();
                        taskQueue.add(pendingTask);
                        pendingTasks.put(temp.getNodeID(), taskQueue);
                    }
                }
            }
        }
    }

    public void rescheduleTasksFrom(Integer slaveID) {
        System.out.println("Started rescheduling slave " + slaveID + "'s tasks");
        Iterator nodeIterator = activeSlaves.iterator();
        Node temp = null;
        Queue<Task> taskQueue;
        boolean allSlavesDead = false;
        int count = 0;

        outstandingTaskCount--;
        if (currentTaskFor.containsKey(slaveID)) {
            pendingTasks.get(slaveID).add(currentTaskFor.get(slaveID));
        }

        for (Task pendingTask : pendingTasks.get(slaveID)) {
            do {
                if (allSlavesDead) {
                    System.out.println("All slaves offline! :o");
                    return;
                }

                if (nodeIterator.hasNext()) {
                    temp = (Node) nodeIterator.next();
                } else {
                    nodeIterator = activeSlaves.iterator();
                    count++;
                }

                if (count == 2) {
                    allSlavesDead = true;
                }
            } while (SocketTaskHandler.getInstance().offlineSlaves.contains(temp.getNodeID()));

            allSlavesDead = false;
            count = 0;

            synchronized (pendingTasks) {
                if (!SocketTaskHandler.getInstance().offlineSlaves.contains(temp.getNodeID())) {
                    pendingTask.setExecutorID(temp.getNodeID());
                    if (pendingTasks.containsKey(temp.getNodeID())) {
                        pendingTasks.get(temp.getNodeID()).add(pendingTask);
                    } else {
                        taskQueue = new LinkedBlockingQueue<>();
                        taskQueue.add(pendingTask);
                        pendingTasks.put(temp.getNodeID(), taskQueue);
                    }
                }
            }
        }
        System.out.println("Finished rescheduling slave " + slaveID + "'s tasks");
    }

    public void beginTasks() {
        for (Map.Entry<Integer, Queue<Task>> entry : pendingTasks.entrySet()) {
            if (entry.getValue().size() != 0) {
                outstandingTaskCount++;
                currentTaskFor.put(entry.getKey(), entry.getValue().remove());
                SocketTaskHandler.dispatchTask(Task.convertToRemoteInput(currentTaskFor.get(entry.getKey())));
            }
        }
    }

    public void markTaskComplete(Task task) {
        Queue<Task> taskQueue;

        outstandingTaskCount--;
        if (completedTasks.containsKey(task.getExecutorID())) {
            completedTasks.get(task.getExecutorID()).add(task);
        } else {
            taskQueue = new LinkedBlockingQueue<>();
            taskQueue.add(task);
            completedTasks.put(task.getExecutorID(), taskQueue);
            currentTaskFor.remove(task.getExecutorID());
        }

        synchronized (pendingTasks) {
            if (pendingTasks.get(task.getExecutorID()).size() > 0) {
                outstandingTaskCount++;
                currentTaskFor.put(task.getExecutorID(), pendingTasks.get(task.getExecutorID()).remove());
                SocketTaskHandler.modifyTask(Task.convertToRemoteInput(currentTaskFor.get(task.getExecutorID())));
            }
        }

        System.out.println("Outstanding tasks: " + getOutstandingTaskCount());
        if (getOutstandingTaskCount() == 0 && isMapPhase) {
            isMapPhase = false;
            initializeReduceTasks();
            assignTasks();
            beginTasks();
        } else if (getOutstandingTaskCount() == 0 && !isMapPhase) {
            writeOutput();
        }
    }

    public Task getPendingTaskFor(Node slaveNode) {
        return new Task();
    }

    public HashMap<Integer, Queue<Task>> getCompletedTasks() {
        return completedTasks;
    }

    public HashMap<Integer, Queue<Task>> getPendingTasks() {
        return pendingTasks;
    }

    public List<Task> getAllTasks() {
        return allTasks;
    }

    public int getOutstandingTaskCount() {
        return outstandingTaskCount;
    }

    public void collectTaskOutput(Task task) {
        File localFile;
        synchronized (this) {
            for (Output output : task.getTaskOutput()) {
                localFile = FileSystem.copyFromRemotePath(output.getRemoteDataPath());
                parseKeyValuePair(localFile, task.getType());
            }
        }
    }

    private void parseKeyValuePair(File intermediateFile, Task.Type type) {
        BufferedReader bufferedReader;
        String temp;
        PrintWriter printWriter;
        File intermediateChunk;

        if (type == Task.Type.MAP) {
            try {
                bufferedReader = new BufferedReader(new FileReader(intermediateFile));
                while ((temp = bufferedReader.readLine()) != null) {
                    intermediateChunk = new File(intermediateDir, "key_" + temp.charAt(0));
                    printWriter = new PrintWriter(new FileWriter(intermediateChunk, true));
                    printWriter.println(temp);
                    printWriter.flush();
                    printWriter.close();
                }
                bufferedReader.close();
            } catch (java.io.IOException e) {
                e.printStackTrace();
            }
        } else {
            reduceKeyValuePair(intermediateFile);
        }
    }

    private void reduceKeyValuePair(File file) {
        String temp, key = null, value = null;
        Splitter splitter;
        Integer valueFinal;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            while ((temp = bufferedReader.readLine()) != null) {
                splitter = new Splitter(temp);
                key = splitter.getKey();
                value = splitter.getValue();
                if (reduceKeys.containsKey(key)) {
                    valueFinal = reduceKeys.get(key) + Integer.valueOf(value);
                    reduceKeys.put(key, valueFinal);
                } else {
                    reduceKeys.put(key, Integer.valueOf(value));
                }
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NumberFormatException e)   {
            System.out.println("File name: " + file.getAbsolutePath() + " Key: " + key + " Value: " + value);
        }
    }

    private void writeOutput() {
        PrintWriter printWriter;
        try {
            printWriter = new PrintWriter(new FileWriter(jobOutput.getLocalFile(), true));
            for (Map.Entry<String, Integer> keyValuePair : reduceKeys.entrySet()) {
                printWriter.println(keyValuePair.getKey() + " " + keyValuePair.getValue());
            }
            printWriter.flush();
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
