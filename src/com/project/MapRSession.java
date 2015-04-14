package com.project;

import com.project.mapr.JobTracker;
import com.project.utils.Input;
import com.project.utils.Node;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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
    private String kafkaHost = "ece-acis-dc282.acis.ufl.edu";
    private String cassandraHost = "ece-acis-dc282.acis.ufl.edu:9160";
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
        String temp;
        StringTokenizer stringTokenizer;
        try {
            if (masterConfig.exists()) {
                configReader = new BufferedReader(new FileReader(masterConfig));
                if ((temp = configReader.readLine()) != null) {
                    temp = new StringTokenizer(temp, " ").nextToken();
                    if (temp.equals(nodeID + "")) {
                        activeNode = new Node(Node.Type.MASTER, nodeID);
                    } else {
                        activeNode = new Node(Node.Type.SLAVE, nodeID);
                    }
                }
            }

            if (slaveConfig.exists()) {
                configReader = new BufferedReader(new FileReader(slaveConfig));
                while ((temp = configReader.readLine()) != null) {
                    stringTokenizer = new StringTokenizer(temp, " ");
                    ResourceManager.slaveAddresses.put(Integer.valueOf(stringTokenizer.nextToken()),
                            stringTokenizer.nextToken());
                }

                if(activeNode.getType() == Node.Type.MASTER)
                    ResourceManager.configureResourceManager(ResourceManager.slaveAddresses);
            }
        } catch (IOException e) {
            e.printStackTrace();
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

    public String getKafkaHost() {
        return kafkaHost;
    }

    public static void exit(int status) {
        System.exit(status);
    }
}
