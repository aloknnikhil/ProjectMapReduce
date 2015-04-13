package com.project;

import com.project.utils.LogFile;
import com.project.utils.Node;
import com.project.utils.Task;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class ResourceManager {

    private ZooKeeper zooKeeper;
    private static final int SESSION_TIMEOUT = 65000;
    private static ResourceManager resourceManagerInstance;

    public final static String APPLICATION_ROOT_PATH = "/mapr";
    public final static String SLAVES_ROOT_PATH = "/slaves";
    public final static String MASTER_ROOT_PATH = "/masters";
    public final static String TASKS_ROOT_PATH = "/tasks";

    public final static String IDLE_SLAVES_PATH = "/idle";
    public final static String BUSY_SLAVES_PATH = "/busy";
    public final static String ALL_SLAVES_PATH = "/all";

    private ResourceManager() {
        try {
            zooKeeper = new ZooKeeper(MapRSession.getInstance().getZookeeperHost(), SESSION_TIMEOUT, new EventWatcher());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Wait for ZooKeeper to finish connecting;
        while (!zooKeeper.getState().isConnected()) {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        LogFile.writeToLog("Connected to Zookeeper Configuration Manager");
    }

    public static void configureResourceManager(List<Integer> slaveIDs) {

        createZNode(APPLICATION_ROOT_PATH, "MapReduceRoot".getBytes(),
                CreateMode.PERSISTENT);

        createZNode(APPLICATION_ROOT_PATH + MASTER_ROOT_PATH, "MastersRoot".getBytes(),
                CreateMode.PERSISTENT);

        createZNode(APPLICATION_ROOT_PATH + TASKS_ROOT_PATH, "TasksRoot".getBytes(),
                CreateMode.PERSISTENT);

        createZNode(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH, "SlavesRoot".getBytes(),
                CreateMode.PERSISTENT);

        createZNode(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH,
                "IdleSlaves".getBytes(), CreateMode.PERSISTENT);

        createZNode(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + BUSY_SLAVES_PATH,
                "BusySlaves".getBytes(), CreateMode.PERSISTENT);

        createZNode(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + ALL_SLAVES_PATH,
                "AllSlaves".getBytes(), CreateMode.PERSISTENT);

        for (Integer slaveID : slaveIDs) {
            createZNode(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + ALL_SLAVES_PATH + "/" + slaveID,
                    ("Slave" + slaveID).getBytes(), CreateMode.PERSISTENT);
        }
    }

    public static List<String> getIdleSlavePaths() {
        try {
            List<String> idleSlaves = getInstance().zooKeeper.getChildren(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH
                    + IDLE_SLAVES_PATH, false);
            return idleSlaves;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static List<String> getBusySlavePaths() {
        try {
            List<String> busySlaves = getInstance().zooKeeper.getChildren(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH
                    + BUSY_SLAVES_PATH, false);
            return busySlaves;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static List<String> getAllSlavePaths() {
        try {
            List<String> allSlaves = getInstance().zooKeeper.getChildren(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH
                    + ALL_SLAVES_PATH, false);
            return allSlaves;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String getMasterPath() {
        try {
            return getInstance().zooKeeper.getChildren(APPLICATION_ROOT_PATH + MASTER_ROOT_PATH, false).get(0);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Node getNodeFrom(String nodePath) {
        try {
            return Node.deserialize(getInstance().zooKeeper.getData(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH
                    + "/"  + nodePath, false, null));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void registerNode(Node node) {
        switch (node.getType()) {
            case MASTER:
                createZNode(APPLICATION_ROOT_PATH + MASTER_ROOT_PATH + "/" + node.getNodeID(),
                        Node.serialize(node), CreateMode.EPHEMERAL);
                break;

            case SLAVE:
                createZNode(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH + "/" + node.getNodeID(),
                        Node.serialize(node), CreateMode.EPHEMERAL);
                setWatcherOn(APPLICATION_ROOT_PATH + TASKS_ROOT_PATH + "/" + node.getNodeID(), node);
                break;
        }
    }

    public static void changeNodeState(int nodeID, Node.Status status) {
        try {
            switch (status) {
                case STARTUP:
                    createZNode(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH + "/" + nodeID,
                            (nodeID + "").getBytes(), CreateMode.EPHEMERAL);
                    break;

                case IDLE:
                    getInstance().zooKeeper.delete(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH
                            + BUSY_SLAVES_PATH + "/" + nodeID, -1);
                    createZNode(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH + "/" + nodeID,
                            (nodeID + "").getBytes(), CreateMode.EPHEMERAL);
                    break;

                case BUSY:
                    getInstance().zooKeeper.delete(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH
                            + IDLE_SLAVES_PATH + "/" + nodeID, -1);
                    createZNode(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + BUSY_SLAVES_PATH + "/" + nodeID,
                            (nodeID + "").getBytes(), CreateMode.EPHEMERAL);
                    break;

                case OFFLINE:
                    createZNode(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + ALL_SLAVES_PATH + "/" + nodeID,
                            (nodeID + "").getBytes(), CreateMode.PERSISTENT);

                    if (getInstance().zooKeeper.exists(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH
                            + BUSY_SLAVES_PATH + "/" + nodeID, false)
                            == null)
                        getInstance().zooKeeper.delete(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH
                                + BUSY_SLAVES_PATH + "/" + nodeID, -1);

                    else if (getInstance().zooKeeper.exists(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH
                            + IDLE_SLAVES_PATH + "/" + nodeID, false)
                            == null)
                        getInstance().zooKeeper.delete(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH
                                + IDLE_SLAVES_PATH + "/" + nodeID, -1);
                    break;
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public static void dispatchTask(Task task) {
        createZNode(APPLICATION_ROOT_PATH + TASKS_ROOT_PATH + "/" + task.getExecutorID(),
                Task.serialize(task), CreateMode.EPHEMERAL);
        setWatcherOn(APPLICATION_ROOT_PATH + TASKS_ROOT_PATH + "/" + task.getExecutorID(),
                MapRSession.getInstance().getActiveNode());
    }

    public static void modifyTask(Task task) {
        try {
            getInstance().zooKeeper.setData(APPLICATION_ROOT_PATH + TASKS_ROOT_PATH
                    + "/" + task.getExecutorID(), Task.serialize(task), -1);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static Task getActiveTaskFor(String nodePath) {
        try {
            return Task.deserialize(getInstance().zooKeeper.getData(nodePath, false, null));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static ResourceManager getInstance() {
        if (resourceManagerInstance == null) {
            resourceManagerInstance = new ResourceManager();
        }
        return resourceManagerInstance;
    }

    private class EventWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
        }
    }

    private static void createZNode(String path, byte[] data, CreateMode createMode) {

        try {
            if (getInstance().zooKeeper.exists(path, false) == null)
                getInstance().zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void setWatcherOn(String path, Node node) {
        try {
            getInstance().zooKeeper.exists(path, node.getTaskWatcher());
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
