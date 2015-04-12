package com.project.mapr;

import com.project.MapRSession;
import com.project.ResourceManager;
import com.project.application.Mapper;
import com.project.application.OutputCollector;
import com.project.application.Reducer;
import com.project.storage.FileSystem;
import com.project.utils.Node;
import com.project.utils.Output;
import com.project.utils.Task;
import javafx.util.Pair;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class TaskTracker implements OutputCollector {

    private Mapper mapper;
    private Reducer reducer;
    public TaskWatcher taskWatcher;
    private File intermediateFile;

    public TaskTracker()    {
        taskWatcher = new TaskWatcher();
        intermediateFile = new File("intermediatefile");
    }

    public class TaskWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {

            switch (event.getType()) {
                case NodeCreated:
                    Task task = ResourceManager.getActiveTaskFor(event.getPath());
                    switch (task.getStatus()) {
                        case INITIALIZED:
                            if(task.getType() == Task.Type.MAP)
                                task.setStatus(Task.Status.RUNNING);
                                ResourceManager.modifyTask(task);
                                runMap(task);
                            break;
                    }
                    break;
            }
        }
    }

    private void runMap(Task task)   {
        File file = FileSystem.copyFromRemotePath(task.getTaskInput().getRemoteDataPath());
        mapper.map(file, this);
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

    private void finishTask(Task task)   {
        task.setTaskOutput(new Output(FileSystem.copyFromLocalFile(intermediateFile)));
        task.setStatus(Task.Status.COMPLETE);
        ResourceManager.modifyTask(task);
        ResourceManager.changeNodeState(MapRSession.getInstance().getActiveNode().getNodeID(),
                Node.Status.IDLE);
    }
}
