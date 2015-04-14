package com.project.mapr;

import com.project.MapRSession;
import com.project.ResourceManager;
import com.project.SocketTaskHandler;
import com.project.storage.FileSystem;
import com.project.utils.Input;
import com.project.utils.Node;
import com.project.utils.Output;
import com.project.utils.Task;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by alok on 4/11/15.
 */
public class JobTracker implements Serializable {

    private HashMap<Integer, Queue<Task>> completedTasks;
    private List<Task> pendingTasks;
    private HashMap<Integer, Queue<Task>> allTasks;
    private List<Node> activeSlaves;
    private Input jobInput;
    private Output jobOutput;
    private int outstandingTaskCount = 0;
    private boolean isMapPhase = true;

    private File intermediateDir = new File("intermediate");

    public JobTracker(Input inputFile) {
        completedTasks = new HashMap<>();
        allTasks = new HashMap<>();
        pendingTasks = new ArrayList<>();
        jobInput = inputFile;
        jobOutput = new Output(new File("results.txt"));
        if (!intermediateDir.exists())
            intermediateDir.mkdir();
    }

    public void start() {
        checkForSlaves();
        SocketTaskHandler.getInstance().connectToSlaves();
        initializeMapTasks();
        System.out.println("Pending Tasks: " + pendingTasks.size());
        assignTasks();
        beginTasks();
    }

    private void checkForSlaves() {
        List<Node> slaveNodes = new ArrayList<>();
        for(Map.Entry<Integer, String> entry : ResourceManager.slaveAddresses.entrySet())   {
            slaveNodes.add(new Node(Node.Type.SLAVE, entry.getKey()));
        }
        this.activeSlaves = slaveNodes;
    }

    public void initializeMapTasks() {
        Task task;
        Input taskInput;
        int count = 1;
        pendingTasks.clear();

        for (File file : jobInput.getLocalFile().listFiles()) {
            if (file.isDirectory()) {
                for (File subFile : file.listFiles()) {
                    task = new Task();
                    task.setType(Task.Type.MAP);
                    task.setStatus(Task.Status.INITIALIZED);
                    task.setTaskID(count);
                    taskInput = new Input(subFile);
                    task.setTaskInput(taskInput);
                    pendingTasks.add(task);
                    count++;
                }
            } else {
                task = new Task();
                task.setType(Task.Type.MAP);
                task.setStatus(Task.Status.INITIALIZED);
                task.setTaskID(count);
                taskInput = new Input(file);
                task.setTaskInput(taskInput);
                pendingTasks.add(task);
                count++;
            }
        }
    }

    public void initializeReduceTasks() {
        Task task;
        Input taskInput;
        int count = 1;

        outstandingTaskCount = 0;
        pendingTasks.clear();
        allTasks.clear();
        completedTasks.clear();
        for (File file : intermediateDir.listFiles()) {
            if (file.isDirectory()) {
                for (File subFile : file.listFiles()) {
                    task = new Task();
                    task.setType(Task.Type.REDUCE);
                    task.setStatus(Task.Status.INITIALIZED);
                    task.setTaskID(count);
                    taskInput = new Input(subFile);
                    task.setTaskInput(taskInput);
                    pendingTasks.add(task);
                    count++;
                }
            } else {
                task = new Task();
                task.setType(Task.Type.REDUCE);
                task.setStatus(Task.Status.INITIALIZED);
                task.setTaskID(count);
                taskInput = new Input(file);
                task.setTaskInput(taskInput);
                pendingTasks.add(task);
                count++;
            }
        }
    }

    public void assignTasks() {
        Iterator nodeIterator = activeSlaves.iterator();
        Node temp;
        Queue<Task> taskQueue;
        for (Task task : pendingTasks) {
            if (!nodeIterator.hasNext())
                nodeIterator = activeSlaves.iterator();

            temp = (Node) nodeIterator.next();

            task.setExecutorID(temp.getNodeID());
            if (allTasks.containsKey(temp.getNodeID())) {
                allTasks.get(temp.getNodeID()).add(task);
            } else {
                taskQueue = new LinkedBlockingQueue<>();
                taskQueue.add(task);
                allTasks.put(temp.getNodeID(), taskQueue);
            }
        }
    }

    public void beginTasks() {
        for (Map.Entry<Integer, Queue<Task>> entry : allTasks.entrySet()) {
            if (entry.getValue().size() != 0) {
                outstandingTaskCount++;
                if (entry.getValue().peek().getType() == Task.Type.MAP) {
                    SocketTaskHandler.dispatchTask(Task.toRemoteInput(entry.getValue().remove()));
                }
                else if (entry.getValue().peek().getType() == Task.Type.REDUCE)
                    SocketTaskHandler.modifyTask(Task.toRemoteInput(entry.getValue().remove()));
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
        }

        if (allTasks.get(task.getExecutorID()).size() > 0) {
            outstandingTaskCount++;
            SocketTaskHandler.modifyTask(Task.toRemoteInput(allTasks.get(task.getExecutorID()).remove()));
        }

        if (getOutstandingTaskCount() == 0 && isMapPhase) {
            isMapPhase = false;
            initializeReduceTasks();
            assignTasks();
            beginTasks();
        }
    }

    public Task getPendingTaskFor(Node slaveNode) {
        return new Task();
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

    public int getOutstandingTaskCount() {
        return outstandingTaskCount;
    }

    public void collectTaskOutput(Task task) {
        File localFile;
        synchronized (this) {
            localFile = FileSystem.copyFromRemotePath(task.getTaskOutput().getRemoteDataPath());
            parseKeyValuePair(localFile, task.getType());
        }
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
