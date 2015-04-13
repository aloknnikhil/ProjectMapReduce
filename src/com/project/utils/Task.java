package com.project.utils;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;

import java.io.*;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class Task implements Serializable, Encoder<Task>, Decoder<Task> {

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

    public Task()   {

    }

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

    @Override
    public Task fromBytes(byte[] bytes) {
        return deserialize(bytes);
    }

    @Override
    public byte[] toBytes(Task task) {
        return serialize(task);
    }

    public static Task deserialize(byte[] bytes) {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o;
        try {
            o = new ObjectInputStream(b);
            return (Task) o.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static byte[] serialize(Task task) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o;
        try {
            o = new ObjectOutputStream(b);
            o.writeObject(task);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return b.toByteArray();
    }

    @Override
    public boolean equals(Object task) {
        if(task instanceof Task)
            return ((Task) task).getTaskID() == this.getTaskID();
        else
            return false;
    }
}
