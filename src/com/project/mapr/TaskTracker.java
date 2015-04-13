package com.project.mapr;

import com.project.MapRSession;
import com.project.ResourceManager;
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

    public TaskTracker() {
        mapper = new WordCount();
        reducer = new WordCount();
        intermediateFile = new File(MapRSession.getRootDir(), "intermediatefile");
    }

    public void runMap(Task task) {
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.BUSY);
        File file = FileSystem.copyFromRemotePath(task.getTaskInput().getRemoteDataPath());
        mapper.map(file, this);
        finishTask(task);
    }

    public void runReduce(Task task) {
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.BUSY);
        File file = FileSystem.copyFromRemotePath(task.getTaskInput().getRemoteDataPath());
        List<Integer> values = new ArrayList<>();
        String temp;
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
            PrintWriter printWriter = new PrintWriter(new FileWriter(intermediateFile, true));
            printWriter.println(keyValuePair.getKey() + ":" + keyValuePair.getValue());
            printWriter.flush();
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void finishTask(Task task) {
        task.setTaskOutput(new Output(FileSystem.copyFromLocalFile(intermediateFile)));
        task.setStatus(Task.Status.COMPLETE);
        ResourceManager.modifyTask(task);
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.IDLE);
    }
}
