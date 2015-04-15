package com.project.utils;

import com.project.storage.*;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class Task implements Serializable, Encoder<Task>, Decoder<Task> {

    public enum Type   {
        MAP,
        HEARTBEAT,
        ACK,
        REDUCE
    }

    public enum Status {
        INITIALIZED,
        RUNNING,
        COMPLETE
    }

    private int taskID;
    private int executorID;
    private List<Input> taskInput;
    private List<Output> taskOutput;
    private Type type;
    private Status status;

    public Task()   {
        taskInput = new ArrayList<>();
        taskOutput = new ArrayList<>();
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

    public List<Input> getTaskInput() {
        return taskInput;
    }

    public void setTaskInput(List<Input> taskInput) {
        this.taskInput = taskInput;
    }

    public void addTaskInput(Input taskInput)   {
        this.taskInput.add(taskInput);
    }

    public List<Output> getTaskOutput() {
        return taskOutput;
    }

    public void setTaskOutput(List<Output> taskOutput) {
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

    public static Task convertToRemoteInput(Task task) {
        if(task.getTaskInput().get(0).getType() == Input.Type.LOCAL) {
            for(Input input : task.getTaskInput()) {
                input.setType(Input.Type.REMOTE);
                input.setRemoteDataPath(FileSystem.copyFromLocalFile(input.getLocalFile()));
                input.setLocalFile(null);
            }
        }
        return task;
    }

    public static Task convertToRemoteOutput(Task task) {
        if(task.getTaskOutput().get(0).getType() == Output.Type.LOCAL) {
            for(Output output : task.getTaskOutput()) {
                output.setType(Output.Type.REMOTE);
                output.setRemoteDataPath(FileSystem.copyFromLocalFile(output.getLocalFile()));
                output.setLocalFile(null);
            }
        }
        return task;
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
