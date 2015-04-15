package com.project.mapr;

import com.project.MapRSession;
import com.project.ResourceManager;
import com.project.SocketTaskHandler;
import com.project.application.Mapper;
import com.project.application.OutputCollector;
import com.project.application.Reducer;
import com.project.application.WordCount;
import com.project.storage.FileSystem;
import com.project.utils.*;
import javafx.util.Pair;

import java.io.*;
import java.util.*;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class TaskTracker implements OutputCollector, Serializable {

    private Mapper mapper;
    private Reducer reducer;
    private File intermediateFile;
    private List<Output> taskOutput;
    private boolean newTask = true;

    public TaskTracker() {
        mapper = new WordCount();
        reducer = new WordCount();
        taskOutput = new ArrayList<>();
    }

    public void runMap(Task task) {
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.BUSY);
        File file;
        intermediateFile = new File(MapRSession.getRootDir(), task.getType() + "_"
                + task.getExecutorID() + "_" + task.getTaskID());
        newTask = true;

        for(Input input : task.getTaskInput()) {
            file = FileSystem.copyFromRemotePath(input.getRemoteDataPath());
            mapper.map(file, this);
        }
        finishTask(task);
    }

    public void runReduce(Task task) {
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.BUSY);
        File file;
        HashMap<String, List<Integer>> keyValuePairs = new HashMap<>();
        String temp, key, value;
        List<Integer> tempList;
        Splitter splitter;
        intermediateFile = new File(MapRSession.getRootDir(), task.getType() + "_"
                + task.getExecutorID() + "_" + task.getTaskID());
        newTask = true;
        for(Input input : task.getTaskInput()) {
            try {
                file = FileSystem.copyFromRemotePath(input.getRemoteDataPath());
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                while ((temp = bufferedReader.readLine()) != null) {
                    splitter = new Splitter(temp);
                    key = splitter.getKey();
                    value = splitter.getValue();
                    if(keyValuePairs.containsKey(key)) {
                        keyValuePairs.get(key).add(Integer.valueOf(value));
                        keyValuePairs.put(key, keyValuePairs.get(key));
                    }
                    else {
                        tempList = new ArrayList<>();
                        tempList.add(Integer.valueOf(value));
                        keyValuePairs.put(key, tempList);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        for(Map.Entry<String, List<Integer>> keyValuePair : keyValuePairs.entrySet())    {
            reducer.reduce(keyValuePair.getKey(), keyValuePair.getValue().iterator(), this);
        }
        finishTask(task);
    }

    @Override
    public void collect(Pair<String, Integer> keyValuePair) {
        Output output;
        File intermediateChunk;
        try {
            intermediateChunk = new File(intermediateFile.getAbsolutePath() + "_" + (int) keyValuePair.getKey().charAt(0));
            PrintWriter printWriter = new PrintWriter(new FileWriter(intermediateChunk, !newTask));
            output = new Output(intermediateChunk);

            if(!taskOutput.contains(output))    {
                taskOutput.add(output);
            }

            printWriter.println(keyValuePair.getKey() + Splitter.DELIMITER + keyValuePair.getValue());
            printWriter.flush();
            printWriter.close();
            newTask = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void finishTask(Task task) {
        task.setTaskOutput(taskOutput);
        task = Task.convertToRemoteOutput(task);
        task.setStatus(Task.Status.COMPLETE);
        SocketTaskHandler.modifyTask(task);
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.IDLE);
    }
}
