package com.project.mapr.scheduler;

import com.project.utils.Input;
import com.project.utils.Task;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by alok on 4/14/15 in ProjectMapReduce
 */
public class RoundRobin implements Scheduler {

    Input jobInput;
    List<Task> pendingTasks;

    public RoundRobin(Input jobInput)   {
        jobInput = this.jobInput;
        pendingTasks = new ArrayList<>();
    }

    private List<Task> getMapTasks() {
        Task task;
        Input taskInput;
        int count = 1;
        pendingTasks.clear();

        for (File file : jobInput.getLocalFile().listFiles()) {
            if (file.isDirectory()) {
                for (File subFile : file.listFiles()) {
                    task = new Task();
                    task.setType(Task.Type.MAP);
                    task.setStatus(Task.Status.INITIALIZED);
                    task.setTaskID(count);
                    taskInput = new Input(subFile);
                    task.setTaskInput(taskInput);
                    pendingTasks.add(task);
                    count++;
                }
            } else {
                task = new Task();
                task.setType(Task.Type.MAP);
                task.setStatus(Task.Status.INITIALIZED);
                task.setTaskID(count);
                taskInput = new Input(file);
                task.setTaskInput(taskInput);
                pendingTasks.add(task);
                count++;
            }
        }

        return pendingTasks;
    }

    private void assignMapTasks() {

    }

    @Override
    public void beginMapTask() {

    }
}
