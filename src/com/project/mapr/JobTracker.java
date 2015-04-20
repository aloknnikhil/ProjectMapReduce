package com.project.mapr;

import com.project.ResourceManager;
import com.project.SocketTaskHandler;
import com.project.storage.FileSystem;
import com.project.utils.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by alok on 4/11/15
 */
public class JobTracker implements Serializable {

    private List<Task> scheduledMapTasks;
    private HashMap<Integer, Queue<Task>> backupPendingTasks;
    private HashMap<Integer, Queue<Task>> pendingReduceTasks;
    private HashMap<Integer, Queue<Task>> pendingMapTasks;
    private List<Integer> pendingResults;
    public List<Node> activeSlaves;
    private Input jobInput;
    private Output jobOutput;
    private int runningTasksCount = 0;
    private int completedTasksCount = 0;
    private boolean isMapPhase = true;

    private File intermediateDir = new File("out/intermediate");

    public JobTracker(Input inputFile) {
        pendingMapTasks = new HashMap<>();
        scheduledMapTasks = new ArrayList<>();
        pendingReduceTasks = new HashMap<>();
        pendingResults = new ArrayList<>();
        jobInput = inputFile;
        jobOutput = new Output(new File("results.txt"));
        if (!intermediateDir.exists())
            intermediateDir.mkdir();
        LogFile.writeToLog("Job Tracker initialized. Output will be written to /results.txt");
    }

    public void start() {
        checkForSlaves();
        SocketTaskHandler.getInstance().connectToSlaves();
        LogFile.writeToLog("All slaves connected");
        LogFile.writeToLog("Started partitioning map tasks");
        initializeMapTasks();
        LogFile.writeToLog("Map tasks have been created");
        assignMapTasks();
        LogFile.writeToLog("Job Scheduler has assigned map tasks");
        LogFile.writeToLog("Map phase has begun");
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
                    LogFile.writeToLog("All slaves offline! :o");
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
        LogFile.writeToLog("Making a copy of the assigned tasks");
        backupPendingTasks = new HashMap<>(pendingTasks);

        for (final Map.Entry<Integer, Queue<Task>> entry : pendingTasks.entrySet()) {
            if (entry.getValue().size() != 0) {
                runningTasksCount++;
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        SocketTaskHandler.dispatchTask(Task.convertToRemoteInput(entry.getValue().remove()));
                    }
                }).start();
            }
        }
    }

    public void rescheduleTasksFrom(Integer slaveID) {
        HashMap<Integer, Queue<Task>> pendingTasks = isMapPhase ? pendingMapTasks : pendingReduceTasks;
        Iterator<Task> deadTasks = backupPendingTasks.get(slaveID).iterator();
        Iterator nodeIterator = activeSlaves.iterator();
        Node temp = null;
        Task pendingTask;
        Queue<Task> taskQueue;
        boolean allSlavesDead = false;
        int count = 0;

        while (deadTasks.hasNext()) {
            pendingTask = deadTasks.next();
            do {
                if (allSlavesDead) {
                    LogFile.writeToLog("All slaves offline! :o");
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

    public void markTaskComplete(Task task) {
        HashMap<Integer, Queue<Task>> pendingTasks = isMapPhase ? pendingMapTasks : pendingReduceTasks;

        synchronized (pendingTasks) {
            if (task.getStatus() != Task.Status.END) {
                runningTasksCount--;
                completedTasksCount++;
            }

            if (pendingTasks.get(task.getCurrentExecutorID()).size() > 0) {
                runningTasksCount++;
                SocketTaskHandler.modifyTask(Task.convertToRemoteInput(
                        pendingTasks.get(task.getCurrentExecutorID()).remove()));
            }

            if(task.getType() == Task.Type.MAP)
                LogFile.writeToLog("Map progress: " + completedTasksCount/pendingTasks.size()
                        + "% Pending tasks: " + (pendingTasks.size() - completedTasksCount));
            else
                LogFile.writeToLog("Reduce progress: " + completedTasksCount/pendingTasks.size()
                        + "% Pending tasks: " + (pendingTasks.size() - completedTasksCount));
        }

        if (getRunningTasksCount() == 0) {
            if(isMapPhase)
                LogFile.writeToLog("Completed Map phase. Consolidating reduce tasks");
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

    public HashMap<Integer, Queue<Task>> getPendingMapTasks() {
        return pendingMapTasks;
    }

    public List<Task> getScheduledMapTasks() {
        return scheduledMapTasks;
    }

    public int getRunningTasksCount() {
        return runningTasksCount;
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
