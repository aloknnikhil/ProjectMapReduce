package com.project;

import com.project.mapr.JobTracker;
import com.project.utils.Input;
import com.project.utils.Node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alok on 4/11/15 in ProjectMapReduce
 */
public class MapRSession {

    private static MapRSession mapRSessionInstance;
    private File sessionDir;
    private Input input;
    private Node activeNode;
    private int nodeID;
    private String zookeeperHost = "ece-acis-dc282.acis.ufl.edu:1499";
    private String cassandraHost = "ece-acis-dc282.acis.ufl.edu";
    public static boolean flag = false;

    public static void main(String[] args) {
        mapRSessionInstance = new MapRSession();
        mapRSessionInstance.startSession(args);
    }

    public void startSession(String[] args) {
        nodeID = Integer.parseInt(args[0]);
        configureSession();
        configureActiveNode();
    }

    private void configureSession() {
        sessionDir = new File("session" + nodeID + "_" + System.currentTimeMillis());
        sessionDir.mkdir();
        input = new Input(new File("input"));
    }

    private void configureActiveNode() {
        File masterConfig = new File("master");
        File slaveConfig = new File("slaves");
        BufferedReader configReader;
        List<Integer> slaveIDs = new ArrayList<>();
        String temp;
        if (masterConfig.exists()) {
            try {
                configReader = new BufferedReader(new FileReader(masterConfig));
                if ((temp = configReader.readLine()) != null) {
                    if (temp.equals(nodeID + "")) {
                        activeNode = new Node(Node.Type.MASTER, nodeID);
                        if (slaveConfig.exists()) {
                            configReader = new BufferedReader(new FileReader(slaveConfig));
                            while ((temp = configReader.readLine()) != null) {
                                slaveIDs.add(Integer.valueOf(temp));
                            }
                            ResourceManager.configureResourceManager(slaveIDs);
                        }
                    } else {
                        activeNode = new Node(Node.Type.SLAVE, nodeID);
                    }
                }
            } catch (java.io.IOException e) {
                e.printStackTrace();
            }
        } else {
            activeNode = new Node(Node.Type.SLAVE, nodeID);
        }
        activeNode.startNode();
    }

    public Node getActiveNode() {
        return activeNode;
    }

    public static File getRootDir() {
        return getInstance().sessionDir;
    }

    public static MapRSession getInstance() {
        return mapRSessionInstance;
    }

    public Input getInput() {
        return input;
    }

    public String getZookeeperHost() {
        return zookeeperHost;
    }

    public String getCassandraHost() {
        return cassandraHost;
    }

    public static void exit(int status) {
        System.exit(status);
    }

}
