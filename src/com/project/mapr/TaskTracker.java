package com.project.mapr;

import com.project.MapRSession;
import com.project.ResourceManager;
import com.project.SocketTaskHandler;
import com.project.application.Mapper;
import com.project.application.OutputCollector;
import com.project.application.Reducer;
import com.project.application.WordCount;
import com.project.storage.FileSystem;
import com.project.utils.Node;
import com.project.utils.Output;
import com.project.utils.Task;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class TaskTracker implements OutputCollector, Serializable {

    private Mapper mapper;
    private Reducer reducer;
    private File intermediateFile;
    private HashMap<String, List<Integer>> reduceKeyValuePairs;
    private HashMap<Integer, String> destinationPartitions;
    private Task currentTask;

    public TaskTracker() {
        mapper = new WordCount();
        reducer = new WordCount();
        reduceKeyValuePairs = new HashMap<>();
        destinationPartitions = new HashMap<>();
    }

    public void runMap(Task task) {
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.BUSY);
        currentTask = task;
        File file = FileSystem.copyFromRemotePath(task.getTaskInput().getRemoteDataPath());
        mapper.map(file, this);
        finishTask(task);
    }

    public void runReduce(final Task task) {
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.BUSY);
        currentTask = task;
        File file = FileSystem.copyFromRemotePath(task.getTaskInput().getRemoteDataPath());
        List<Integer> values;
        String temp, key, value;
        StringTokenizer stringTokenizer;
        intermediateFile = new File(MapRSession.getRootDir(), task.getType() + "_" + task.getCurrentExecutorID());
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            while ((temp = bufferedReader.readLine()) != null) {
                stringTokenizer = new StringTokenizer(temp, ":");
                key = stringTokenizer.nextToken();
                value = stringTokenizer.nextToken();
                if (reduceKeyValuePairs.containsKey(key)) {
                    reduceKeyValuePairs.get(key).add(Integer.valueOf(value));
                } else {
                    values = new ArrayList<>();
                    values.add(Integer.valueOf(value));
                    reduceKeyValuePairs.put(key, values);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        finishTask(task);
    }

    @Override
    public void collect(Pair<String, Integer> keyValuePair) {
        int partitionID = ResourceManager.getPartitionForKey(keyValuePair.getKey());
        try {
            if(currentTask.getType() == Task.Type.MAP) {
                intermediateFile = new File(MapRSession.getRootDir(), currentTask.getType() + "_"
                        + currentTask.getCurrentExecutorID() + "_"
                        + partitionID);
                if (!destinationPartitions.containsKey(partitionID))
                    destinationPartitions.put(partitionID, intermediateFile.getAbsolutePath());
            }

            PrintWriter printWriter = new PrintWriter(new FileWriter(intermediateFile, true));
            printWriter.println(keyValuePair.getKey() + ":" + keyValuePair.getValue());
            printWriter.flush();
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void finishTask(Task task) {
        task.setStatus(Task.Status.COMPLETE);
        SocketTaskHandler.modifyTask(task);
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.IDLE);
    }

    public void getPhaseOutput(Task task) {
        if (task.getStatus() == Task.Status.COMPLETE) {
            ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                    Node.Status.BUSY);
            task.setStatus(Task.Status.END);

            if(task.getType() == Task.Type.MAP) {
                for(Map.Entry<Integer, String> entry : destinationPartitions.entrySet())    {
                    destinationPartitions.put(entry.getKey(),
                            FileSystem.copyFromLocalFile(new File(entry.getValue())));
                }
                task.setReducePartitionIDs(destinationPartitions);
            } else if(task.getType() == Task.Type.REDUCE) {
                for (final Map.Entry<String, List<Integer>> entry : reduceKeyValuePairs.entrySet()) {
                    reducer.reduce(entry.getKey(), entry.getValue().iterator(), TaskTracker.this);
                }
                task.setTaskOutput(new Output(FileSystem.copyFromLocalFile(intermediateFile)));
            }

            SocketTaskHandler.modifyTask(task);
            ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                    Node.Status.IDLE);
        }
    }
}
