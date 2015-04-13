package com.project.mapr;

import com.project.MapRSession;
import com.project.ResourceManager;
import com.project.TaskHandler;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class TaskTracker implements OutputCollector, Serializable {

    private Mapper mapper;
    private Reducer reducer;
    private File intermediateFile;
    private boolean newTask = true;

    public TaskTracker() {
        mapper = new WordCount();
        reducer = new WordCount();
    }

    public void runMap(Task task) {
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.BUSY);
        File file = FileSystem.copyFromRemotePath(task.getTaskInput().getRemoteDataPath());
        intermediateFile = new File(MapRSession.getRootDir(), task.getType() + "_"
                + task.getExecutorID() + "_" + task.getTaskID());
        newTask = true;
        mapper.map(file, this);
        finishTask(task);
    }

    public void runReduce(Task task) {
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.BUSY);
        File file = FileSystem.copyFromRemotePath(task.getTaskInput().getRemoteDataPath());
        List<Integer> values = new ArrayList<>();
        String temp;
        intermediateFile = new File(MapRSession.getRootDir(), task.getType() + "_"
                + task.getExecutorID() + "_" + task.getTaskID());
        newTask = true;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            while((temp = bufferedReader.readLine()) != null)   {
                values.add(Integer.valueOf(temp));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        reducer.reduce(file.getName(), values.iterator(), this);
        finishTask(task);
    }

    @Override
    public void collect(Pair<String, Integer> keyValuePair) {
        try {
            PrintWriter printWriter = new PrintWriter(new FileWriter(intermediateFile, !newTask));
            printWriter.println(keyValuePair.getKey() + ":" + keyValuePair.getValue());
            printWriter.flush();
            printWriter.close();
            newTask = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void finishTask(Task task) {
        task.setTaskOutput(new Output(FileSystem.copyFromLocalFile(intermediateFile)));
        task.setStatus(Task.Status.COMPLETE);
        TaskHandler.modifyTask(task);
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.IDLE);
    }
}
