package com.alok.mapr;

import com.alok.utils.Node;
import com.alok.utils.Task;

import java.util.HashMap;

/**
 * Created by alok on 4/11/15.
 */
public class JobTracker {

    private static HashMap<String, Task> runningTasks;
    private static HashMap<String, Task> completedTasks;
    private static HashMap<String, Task> allTasks;

    public JobTracker() {
        runningTasks = new HashMap<>();
        completedTasks = new HashMap<>();
        allTasks = new HashMap<>();
    }

    //TODO
    public static Task getTaskFor(Node slaveNode) {
        return new Task();
    }

    public static HashMap<String, Task> getRunningTasks() {
        return runningTasks;
    }

    public static HashMap<String, Task> getCompletedTasks() {
        return completedTasks;
    }

    public static HashMap<String, Task> getAllTasks() {
        return allTasks;
    }

    public static void assignTaskTo(Task task, Node slaveNode)  {

    }
}
