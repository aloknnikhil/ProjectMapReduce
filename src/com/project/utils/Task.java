package com.project.utils;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class Task {

    public enum Type   {
        MAP,
        COMBINE,
        REDUCE
    }

    public enum Status {
        INITIALIZED,
        RUNNING,
        COMPLETE
    }

    private int taskID;
    private int executorID;
    private Input taskInput;
    private Output taskOutput;
    private Type type;
    private Status status;

    public int getTaskID() {
        return taskID;
    }

    public void setTaskID(int taskID) {
        this.taskID = taskID;
    }

    public int getExecutorID() {
        return executorID;
    }

    public void setExecutorID(int executorID) {
        this.executorID = executorID;
    }

    public Input getTaskInput() {
        return taskInput;
    }

    public void setTaskInput(Input taskInput) {
        this.taskInput = taskInput;
    }

    public Output getTaskOutput() {
        return taskOutput;
    }

    public void setTaskOutput(Output taskOutput) {
        this.taskOutput = taskOutput;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }
}
