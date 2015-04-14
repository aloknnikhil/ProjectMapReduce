package com.project.mapr;

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
    private List<Task> allTasks;
    private HashMap<Integer, Task> currentTaskFor;
    private HashMap<Integer, Queue<Task>> pendingTasks;
    private List<Node> activeSlaves;
    private Input jobInput;
    private Output jobOutput;
    private int outstandingTaskCount = 0;
    private boolean isMapPhase = true;

    private File intermediateDir = new File("intermediate");

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
        scheduleTasks(allTasks);
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
                    task.setTaskInput(taskInput);
                    allTasks.add(task);
                    count++;
                }
            } else {
                task = new Task();
                task.setType(Task.Type.MAP);
                task.setStatus(Task.Status.INITIALIZED);
                task.setTaskID(count);
                taskInput = new Input(file);
                task.setTaskInput(taskInput);
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
                    task.setTaskInput(taskInput);
                    allTasks.add(task);
                    count++;
                }
            } else {
                task = new Task();
                task.setType(Task.Type.REDUCE);
                task.setStatus(Task.Status.INITIALIZED);
                task.setTaskID(count);
                taskInput = new Input(file);
                task.setTaskInput(taskInput);
                allTasks.add(task);
                count++;
            }
        }
    }

    public void scheduleTasks(Collection<Task> taskList)  {
        Iterator nodeIterator = activeSlaves.iterator();
        Node temp = null;
        Queue<Task> taskQueue;
        boolean allSlavesDead = false;
        int count = 0;

        for (Task pendingTask : taskList) {
            do {
                if(allSlavesDead) {
                    System.out.println("All slaves offline! :o");
                    return;
                }

                if(nodeIterator.hasNext())  {
                    temp = (Node) nodeIterator.next();
                } else {
                    nodeIterator = activeSlaves.iterator();
                    count++;
                }

                if(count == 2) {
                    allSlavesDead = true;
                }
            } while(SocketTaskHandler.getInstance().offlineSlaves.contains(temp.getNodeID()));

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
            scheduleTasks(allTasks);
            beginTasks();
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
