package com.alok.utils;

import com.alok.Session;
import com.alok.storage.*;

import java.io.*;

/**
 * Created by alok on 4/11/15
 */
public class Node implements Serializable {

    public enum NodeType {
        MASTER,
        SLAVE
    }

    public enum Status {
        STARTUP,
        IDLE,
        BUSY,
        DEAD
    }

    private NodeType type;
    private String nodeID;

    public Node(NodeType type, String nodeID) {
        this.type = type;
        this.nodeID = nodeID;
    }

    public void startNode() {

        //If the com.alok.utils.Node is a master node, then we need additional setup procedures
        if (type == NodeType.MASTER) {
            Session.startJobTracker();
            FileSystem.setFileSystemManager(this);
            FileSystem.startNameNodeService();
        }

        Session.startTaskTracker();
        Session.startDataNode();
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
        ObjectInputStream o = null;
        try {
            o = new ObjectInputStream(b);
            return (Node) o.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}
