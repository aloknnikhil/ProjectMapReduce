package com.project.utils;

import com.project.MapRSession;
import com.project.ResourceManager;
import com.project.mapr.JobTracker;
import com.project.mapr.TaskTracker;
import com.project.storage.FileSystem;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.*;

/**
 * Created by alok on 4/11/15
 */
public class Node implements Serializable, IZkDataListener {

    public enum Type {
        MASTER,
        SLAVE
    }

    public enum Status {
        STARTUP,
        IDLE,
        BUSY,
        OFFLINE
    }

    private Type type;
    private int nodeID;
    private JobTracker jobTracker;
    private TaskTracker taskTracker;

    public Node(Type type, int nodeID) {
        this.type = type;
        this.nodeID = nodeID;
        if (this.type == Type.MASTER)
            jobTracker = new JobTracker(MapRSession.getInstance().getInput());

        taskTracker = new TaskTracker();
    }

    public void startNode() {

        //If the com.alok.utils.Node is a master node, then we need additional setup procedures
        if (type == Type.MASTER) {
            jobTracker.start();
            FileSystem.setFileSystemManager(this);
        }
        ResourceManager.registerNode(this);

        while (true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static byte[] serialize(Node node) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o;
        try {
            o = new ObjectOutputStream(b);
            o.writeObject(node);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return b.toByteArray();
    }

    public static Node deserialize(byte[] bytes) {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o;
        Node node;
        try {
            o = new ObjectInputStream(b);
            node = (Node) o.readObject();
            o.close();
            return node;
        } catch (EOFException e) {
            return null;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public int getNodeID() {
        return nodeID;
    }

    public Type getType() {
        return type;
    }

    @Override
    public void handleDataChange(String path, Object data) throws Exception {
        System.out.println("Got a change event!");
        switch (Node.this.getType()) {
            case MASTER:
                final Task taskMaster = Task.deserialize((byte[]) data);
                switch (taskMaster.getStatus()) {
                    case RUNNING:
                        //TODO Check if the task running time exceeded the timeout period
                        break;

                    case COMPLETE:
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                jobTracker.collectTaskOutput(taskMaster);
                                jobTracker.markTaskComplete(taskMaster);
                                System.out.println("Number of tasks completed: "
                                        + jobTracker.getCompletedTasks().size() + " Number of outstanding tasks: "
                                        + jobTracker.getOutstandingTaskCount());
                            }
                        }).start();
                        break;
                }
                break;

            case SLAVE:
                final Task taskSlave = Task.deserialize((byte[]) data);
                switch (taskSlave.getStatus()) {
                    case INITIALIZED:
                        if (taskSlave.getType() == Task.Type.MAP) {
                            taskSlave.setStatus(Task.Status.RUNNING);
                            ResourceManager.modifyTask(taskSlave);
                            new Thread(new Runnable() {
                                @Override
                                public void run() {
                                    taskTracker.runMap(taskSlave);
                                }
                            }).start();
                        } else if (taskSlave.getType() == Task.Type.REDUCE) {
                            taskSlave.setStatus(Task.Status.RUNNING);
                            ResourceManager.modifyTask(taskSlave);
                            new Thread(new Runnable() {
                                @Override
                                public void run() {
                                    taskTracker.runReduce(taskSlave);
                                }
                            }).start();
                        }
                        break;
                }

        }
    }

    @Override
    public void handleDataDeleted(String s) throws Exception {

    }
}
