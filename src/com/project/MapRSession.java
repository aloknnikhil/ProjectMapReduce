package com.project;

import com.project.utils.Input;
import com.project.utils.LogFile;
import com.project.utils.Node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Alok on 4/11/15 in ProjectMapReduce
 */
public class MapRSession {

    public enum Mode {
        OFFLINE,
        ONLINE
    }

    private static MapRSession mapRSessionInstance;
    private Mode mode;
    private File sessionDir;
    private Input input;
    private Node activeNode;
    private int nodeID;
    private String inputPath;
    private String zookeeperHost = "ece-acis-dc282.acis.ufl.edu:1499";
    private String kafkaHost = "ece-acis-dc282.acis.ufl.edu";
    private String cassandraSeeds = "ece-acis-dc281.acis.ufl.edu:9160";
    public static boolean flag = false;

    public static void main(String[] args) {
        mapRSessionInstance = new MapRSession();
        mapRSessionInstance.startSession(args);
    }

    public void startSession(String[] args) {
        try {
            nodeID = Integer.parseInt(args[0]);
            inputPath = args[1];
        } catch (Exception e)   {
            LogFile.writeToLog("Invalid arguments");
            System.exit(-1);
        }
        LogFile.writeToLog("Current Node ID is " + nodeID);
        configureSession();
        configureActiveNode();
    }

    private void configureSession() {
        sessionDir = new File("out/session" + nodeID + "_" + System.currentTimeMillis());
        sessionDir.mkdir();
        LogFile.writeToLog("Reading input from folder " + inputPath);
        File inputFolder = new File(inputPath);
        if(!inputFolder.exists())   {
            LogFile.writeToLog("No such input directory");
            System.exit(-1);
        }
        input = new Input(inputFolder);
        mode = Mode.OFFLINE;
    }

    private void configureActiveNode() {
        File masterConfig = new File("master");
        File slaveConfig = new File("slaves");
        BufferedReader configReader;
        Integer slaveID;
        String temp, slaveAddress;
        StringTokenizer stringTokenizer;
        try {
            if (masterConfig.exists()) {
                LogFile.writeToLog("Found master configuration file");
                configReader = new BufferedReader(new FileReader(masterConfig));
                if ((temp = configReader.readLine()) != null) {
                    temp = new StringTokenizer(temp, " ").nextToken();
                    if (temp.equals(nodeID + "")) {
                        LogFile.writeToLog("I am the master");
                        activeNode = new Node(Node.Type.MASTER, nodeID);
                    } else {
                        LogFile.writeToLog("I am a slave");
                        activeNode = new Node(Node.Type.SLAVE, nodeID);
                    }
                }
            }

            if (slaveConfig.exists()) {
                LogFile.writeToLog("Found slave configuration file");
                configReader = new BufferedReader(new FileReader(slaveConfig));
                while ((temp = configReader.readLine()) != null) {
                    stringTokenizer = new StringTokenizer(temp, " ");
                    slaveID = Integer.valueOf(stringTokenizer.nextToken());
                    slaveAddress = stringTokenizer.nextToken();
                    LogFile.writeToLog("Found slave " + slaveID + " at " + slaveAddress + " in configuration file");
                    ResourceManager.slaveAddresses.put(slaveID, slaveAddress);
                }

                if(activeNode.getType() == Node.Type.MASTER) {
                    LogFile.writeToLog("Exporting configuration to Zookeeper");
                    ResourceManager.configureResourceManager(ResourceManager.slaveAddresses);
                }
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

    public String getCassandraSeeds() {
        return cassandraSeeds;
    }

    public String getKafkaHost() {
        return kafkaHost;
    }

    public Mode getMode() {
        return mode;
    }

    public static void exit(int status) {
        System.exit(status);
    }
}
