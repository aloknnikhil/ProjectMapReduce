package com.project.mapr;

import com.project.ResourceManager;
import com.project.storage.FileSystem;
import com.project.utils.Input;
import com.project.utils.Node;
import com.project.utils.Task;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.File;
import java.util.*;

/**
 * Created by alok on 4/11/15.
 */
public class JobTracker {

    private HashMap<Integer, Queue<Task>> runningTasks;
    private HashMap<Integer, Queue<Task>> completedTasks;
    private List<Task> pendingTasks;
    private HashMap<Integer, Queue<Task>> allTasks;
    private List<Node> slaveNodes;
    private Input jobInput;
    public static Watcher taskWatcher;

    public JobTracker(Input inputFile, List<Node> slaveNodes) {
        runningTasks = new HashMap<>();
        completedTasks = new HashMap<>();
        allTasks = new HashMap<>();
        pendingTasks = new ArrayList<>();
        taskWatcher = new TaskWatcher();
        jobInput = inputFile;
        this.slaveNodes = slaveNodes;
    }

    public void startScheduler()    {
        initializeMapTasks();
        assignTasks();
        beginTasks();
    }

    private void initializeMapTasks()   {
        Task task;
        Input taskInput;
        int count = 1;
        for(File file : jobInput.getLocalFile().listFiles())   {
            task = new Task();
            task.setType(Task.Type.MAP);
            task.setStatus(Task.Status.INITIALIZED);
            task.setTaskID(count);
            taskInput = new Input(FileSystem.copyFrom(file));
            task.setTaskInput(taskInput);
            pendingTasks.add(task);
            count++;
        }
    }

    private void assignTasks()  {
        Iterator nodeIterator = slaveNodes.iterator();
        Node temp;
        for(Task task : pendingTasks)   {
            if(!nodeIterator.hasNext())
                nodeIterator = slaveNodes.iterator();

            temp = (Node) nodeIterator.next();

            if(allTasks.containsKey(temp.getNodeID()))  {
                task.setExecutorID(temp.getNodeID());
                allTasks.get(temp.getNodeID()).add(task);
            }
        }
    }

    private void beginTasks()   {
        for(Map.Entry<Integer, Queue<Task>> entry : allTasks.entrySet())    {
            ResourceManager.dispatchTask(entry.getValue().peek());
        }
    }

    public Task getPendingTaskFor(Node slaveNode) {
        return new Task();
    }

    public HashMap<Integer, Queue<Task>> getRunningTasks() {
        return runningTasks;
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

    public class TaskWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {

        }
    }
}
