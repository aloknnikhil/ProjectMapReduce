package com.project.utils;

import com.project.MapRSession;
import com.project.ResourceManager;
import com.project.SocketTaskHandler;
import com.project.TaskHandler;
import com.project.mapr.JobTracker;
import com.project.mapr.TaskTracker;
import com.project.storage.FileSystem;
import org.I0Itec.zkclient.IZkDataListener;

import java.io.*;

/**
 * Created by alok on 4/11/15
 */
public class Node implements Serializable, IZkDataListener, TaskChangeListener {

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

    public JobTracker getJobTracker() {
        return jobTracker;
    }

    public TaskTracker getTaskTracker() {
        return taskTracker;
    }

    @Override
    public void handleDataChange(String path, Object data) throws Exception {

    }

    @Override
    public void handleDataDeleted(String s) throws Exception {

    }

    @Override
    public void onTaskChanged(final Task task) {

    }
}
