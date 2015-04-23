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

import java.io.*;
import java.util.*;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class TaskTracker implements OutputCollector, Serializable {

    private Mapper mapper;
    private Reducer reducer;
    private File intermediateFile;
    private HashMap<String, Integer> mapKeyValuePairs;
    private HashMap<String, List<Integer>> reduceKeyValuePairs;
    private HashMap<Integer, String> destinationPartitions;
    private Task currentTask;

    public TaskTracker() {
        mapper = new WordCount();
        reducer = new WordCount();
        mapKeyValuePairs = new HashMap<>();
        reduceKeyValuePairs = new HashMap<>();
        destinationPartitions = new HashMap<>();
    }

    public void runMap(Task task) {
        if (task.getType() == Task.Type.MAP) {
            ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                    Node.Status.BUSY);
            currentTask = task;
            File file;
            if (MapRSession.getInstance().getMode() == MapRSession.Mode.ONLINE)
                file = FileSystem.copyFromRemotePath(task.getTaskInput().getRemoteDataPath());
            else {
                file = new File(task.getTaskInput().getLocalFile().getPath());
            }
            mapper.map(file, this);
            finishTask(task);
        }
    }

    public void runReduce(final Task task) {
        if (task.getType() == Task.Type.REDUCE) {
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
    }

    @Override
    public void collect(String key, Integer value) {
        if (currentTask.getType() == Task.Type.MAP) {
            if (mapKeyValuePairs.containsKey(key))
                mapKeyValuePairs.put(key, mapKeyValuePairs.get(key) + value);
            else
                mapKeyValuePairs.put(key, value);
        } else {
            reduceKeyValuePairs.get(key).clear();
            reduceKeyValuePairs.get(key).add(value);
        }
    }

    private void combineOutput() {
        if (currentTask.getType() == Task.Type.MAP) {
            for (Map.Entry<String, Integer> entry : mapKeyValuePairs.entrySet()) {
                int partitionID = ResourceManager.getPartitionForKey(entry.getKey());
                try {

                    intermediateFile = new File(MapRSession.getRootDir(), currentTask.getType() + "_"
                            + currentTask.getCurrentExecutorID() + "_"
                            + partitionID);
                    if (!destinationPartitions.containsKey(partitionID))
                        destinationPartitions.put(partitionID, intermediateFile.getAbsolutePath());


                    PrintWriter printWriter = new PrintWriter(new FileWriter(intermediateFile, true));
                    printWriter.println(entry.getKey() + ":" + entry.getValue());
                    printWriter.flush();
                    printWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            for (Map.Entry<String, List<Integer>> entry : reduceKeyValuePairs.entrySet()) {
                try {
                    PrintWriter printWriter = new PrintWriter(new FileWriter(intermediateFile, true));
                    printWriter.println(entry.getKey() + ":" + entry.getValue().get(0));
                    printWriter.flush();
                    printWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
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

            if (task.getType() == Task.Type.MAP) {
                combineOutput();
                for (Map.Entry<Integer, String> entry : destinationPartitions.entrySet()) {
                    destinationPartitions.put(entry.getKey(),
                            FileSystem.copyFromLocalFile(new File(entry.getValue())));
                }
                task.setReducePartitionIDs(destinationPartitions);
            } else if (task.getType() == Task.Type.REDUCE) {
                for (final Map.Entry<String, List<Integer>> entry : reduceKeyValuePairs.entrySet()) {
                    reducer.reduce(entry.getKey(), entry.getValue().iterator(), TaskTracker.this);
                }
                combineOutput();
                task.setTaskOutput(new Output(FileSystem.copyFromLocalFile(intermediateFile)));
            }

            SocketTaskHandler.modifyTask(task);
            ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                    Node.Status.IDLE);
        }
    }
}
