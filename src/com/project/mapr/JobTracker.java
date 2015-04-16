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
 * Created by alok on 4/11/15
 */
public class JobTracker implements Serializable {

    private List<Task> scheduledMapTasks;
    private HashMap<Integer, Queue<Task>> pendingReduceTasks;
    private HashMap<Integer, Queue<Task>> pendingMapTasks;
    private HashMap<Integer, Queue<Task>> completedTasks;
    private HashMap<Integer, Task> currentTaskFor;
    private List<Integer> pendingResults;
    public List<Node> activeSlaves;
    private Input jobInput;
    private Output jobOutput;
    private int outstandingTaskCount = 0;
    private boolean isMapPhase = true;

    private File intermediateDir = new File("out/intermediate");

    public JobTracker(Input inputFile) {
        completedTasks = new HashMap<>();
        pendingMapTasks = new HashMap<>();
        currentTaskFor = new HashMap<>();
        scheduledMapTasks = new ArrayList<>();
        pendingReduceTasks = new HashMap<>();
        pendingResults = new ArrayList<>();
        jobInput = inputFile;
        jobOutput = new Output(new File("results.txt"));
        if (!intermediateDir.exists())
            intermediateDir.mkdir();
    }

    public void start() {
        checkForSlaves();
        SocketTaskHandler.getInstance().connectToSlaves();
        initializeMapTasks();
        System.out.println("Pending Tasks: " + scheduledMapTasks.size());
        assignMapTasks();
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
        scheduledMapTasks.clear();

        for (File file : jobInput.getLocalFile().listFiles()) {
            if (file.isDirectory()) {
                for (File subFile : file.listFiles()) {
                    task = new Task();
                    task.setType(Task.Type.MAP);
                    task.setStatus(Task.Status.INITIALIZED);
                    task.setTaskID(count);
                    taskInput = new Input(subFile);
                    task.setTaskInput(taskInput);
                    scheduledMapTasks.add(task);
                    count++;
                }
            } else {
                task = new Task();
                task.setType(Task.Type.MAP);
                task.setStatus(Task.Status.INITIALIZED);
                task.setTaskID(count);
                taskInput = new Input(file);
                task.setTaskInput(taskInput);
                scheduledMapTasks.add(task);
                count++;
            }
        }
    }

    public void assignMapTasks() {
        Iterator nodeIterator = activeSlaves.iterator();
        Node temp = null;
        Queue<Task> taskQueue;
        boolean allSlavesDead = false;
        int count = 0;

        for (Task pendingTask : scheduledMapTasks) {
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

            synchronized (pendingMapTasks) {
                if (!SocketTaskHandler.getInstance().offlineSlaves.contains(temp.getNodeID())) {
                    pendingTask.setCurrentExecutorID(temp.getNodeID());
                    if (pendingMapTasks.containsKey(temp.getNodeID())) {
                        pendingMapTasks.get(temp.getNodeID()).add(pendingTask);
                    } else {
                        taskQueue = new LinkedBlockingQueue<>();
                        taskQueue.add(pendingTask);
                        pendingMapTasks.put(temp.getNodeID(), taskQueue);
                    }
                }
            }
        }
    }

    public void beginTasks() {
        HashMap<Integer, Queue<Task>> pendingTasks = isMapPhase ? pendingMapTasks : pendingReduceTasks;

        for (final Map.Entry<Integer, Queue<Task>> entry : pendingTasks.entrySet()) {
            if (entry.getValue().size() != 0) {
                outstandingTaskCount++;
                currentTaskFor.put(entry.getKey(), entry.getValue().remove());
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        SocketTaskHandler.dispatchTask(Task.convertToRemoteInput(currentTaskFor.get(entry.getKey())));
                    }
                }).start();
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
            pendingMapTasks.get(slaveID).add(currentTaskFor.get(slaveID));
        }

        for (Task pendingTask : pendingMapTasks.get(slaveID)) {
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

            synchronized (pendingMapTasks) {
                if (!SocketTaskHandler.getInstance().offlineSlaves.contains(temp.getNodeID())) {
                    pendingTask.setCurrentExecutorID(temp.getNodeID());
                    if (pendingMapTasks.containsKey(temp.getNodeID())) {
                        pendingMapTasks.get(temp.getNodeID()).add(pendingTask);
                    } else {
                        taskQueue = new LinkedBlockingQueue<>();
                        taskQueue.add(pendingTask);
                        pendingMapTasks.put(temp.getNodeID(), taskQueue);
                    }
                }
            }
        }
        System.out.println("Finished rescheduling slave " + slaveID + "'s tasks");
    }

    public void markTaskComplete(Task task) {
        Queue<Task> taskQueue;
        HashMap<Integer, Queue<Task>> pendingTasks = isMapPhase ? pendingMapTasks : pendingReduceTasks;

        synchronized (currentTaskFor) {
            if (task.getStatus() != Task.Status.END) {
                outstandingTaskCount--;
                System.out.println("Task Status: " + task.getStatus());
            }

            if (completedTasks.containsKey(task.getCurrentExecutorID())) {
                completedTasks.get(task.getCurrentExecutorID()).add(task);
            } else {
                taskQueue = new LinkedBlockingQueue<>();
                taskQueue.add(task);
                completedTasks.put(task.getCurrentExecutorID(), taskQueue);
                currentTaskFor.remove(task.getCurrentExecutorID());
            }
            if (pendingTasks.get(task.getCurrentExecutorID()).size() > 0) {
                outstandingTaskCount++;
                currentTaskFor.put(task.getCurrentExecutorID(), pendingTasks.get(task.getCurrentExecutorID()).remove());
                SocketTaskHandler.modifyTask(Task.convertToRemoteInput(currentTaskFor.get(task.getCurrentExecutorID())));
            }
        }

        if (getOutstandingTaskCount() == 0) {
            collectResults(task.getType());
        }
    }

    private void collectResults(Task.Type type) {
        for (Integer slave : pendingResults) {
            Task tempTask = new Task();
            tempTask.setType(type);
            tempTask.setStatus(Task.Status.COMPLETE);
            tempTask.setCurrentExecutorID(slave);
            SocketTaskHandler.dispatchTask(tempTask);
        }
    }

    public Task getPendingTaskFor(Node slaveNode) {
        return new Task();
    }

    public HashMap<Integer, Queue<Task>> getCompletedTasks() {
        return completedTasks;
    }

    public HashMap<Integer, Queue<Task>> getPendingMapTasks() {
        return pendingMapTasks;
    }

    public List<Task> getScheduledMapTasks() {
        return scheduledMapTasks;
    }

    public int getOutstandingTaskCount() {
        return outstandingTaskCount;
    }

    public void collectTaskOutput(final Task task) {
        Task reduceTask;
        Queue<Task> taskQueue;

        if (task.getStatus() == Task.Status.COMPLETE) {
            synchronized (pendingResults) {
                if (!pendingResults.contains(task.getCurrentExecutorID()))
                    pendingResults.add(task.getCurrentExecutorID());
            }
        } else if (task.getStatus() == Task.Status.END) {
            if (task.getType() == Task.Type.MAP) {
                for (Map.Entry<Integer, String> partitionEntry : task.getReducePartitionIDs().entrySet()) {
                    reduceTask = new Task();
                    reduceTask.setCurrentExecutorID(activeSlaves.get(partitionEntry.getKey()).getNodeID());
                    reduceTask.setTaskInput(new Input(partitionEntry.getValue()));
                    reduceTask.setType(Task.Type.REDUCE);
                    reduceTask.setStatus(Task.Status.INITIALIZED);

                    if (pendingReduceTasks.containsKey(activeSlaves.get(partitionEntry.getKey()).getNodeID()))
                        pendingReduceTasks.get(activeSlaves.get(partitionEntry.getKey()).getNodeID()).add(reduceTask);
                    else {
                        taskQueue = new LinkedBlockingQueue<>();
                        taskQueue.add(reduceTask);
                        pendingReduceTasks.put(activeSlaves.get(partitionEntry.getKey()).getNodeID(), taskQueue);
                    }
                    pendingResults.remove(new Integer(task.getCurrentExecutorID()));
                    if (pendingResults.size() == 0 && isMapPhase) {
                        isMapPhase = false;
                        currentTaskFor.clear();
                        completedTasks.clear();
                        beginTasks();
                    }
                }
            } else {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        File localFile = FileSystem.copyFromRemotePath(task.getTaskOutput().getRemoteDataPath());
                        parseKeyValuePair(localFile);
                    }
                }).start();
            }
        }
    }

    private void parseKeyValuePair(File intermediateFile) {
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
                printWriter = new PrintWriter(new FileWriter(jobOutput.getLocalFile(), true));
                printWriter.println(key + " " + value);

                printWriter.flush();
                printWriter.close();
            }
            bufferedReader.close();
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }
}
