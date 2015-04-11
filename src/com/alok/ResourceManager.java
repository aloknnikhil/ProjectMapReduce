package com.alok;

import com.alok.utils.LogFile;
import com.alok.utils.Node;
import com.alok.utils.Task;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class ResourceManager {

    private static ZooKeeper zooKeeper;
    private static final int SESSION_TIMEOUT = 10000;
    private static ResourceManager resourceManagerInstance;

    public final static String APPLICATION_ROOT_PATH = "/com/alok/mapr";
    public final static String SLAVES_ROOT_PATH = "/slaves";
    public final static String MASTERS_ROOT_PATH = "/masters";
    public final static String IDLE_SLAVES_PATH = "/idle";
    public final static String BUSY_SLAVES_PATH = "/busy";
    public final static String ALL_SLAVES_PATH = "/all";

    private ResourceManager() {
        try {
            zooKeeper = new ZooKeeper("ece-acis-dc281.acis.ufl.edu:1499", SESSION_TIMEOUT, new EventWatcher());
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
        configureResourceManager();
    }

    private void configureResourceManager() {
        try {
            if (zooKeeper.exists(APPLICATION_ROOT_PATH, false) != null) {
                if (zooKeeper.exists(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH, false) != null) {
                    if (zooKeeper.exists(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH, false) == null)
                        zooKeeper.create(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH,
                                "IdleSlaves".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                    if (zooKeeper.exists(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + BUSY_SLAVES_PATH, false) == null)
                        zooKeeper.create(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + BUSY_SLAVES_PATH,
                                "BusySlaves".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                    if (zooKeeper.exists(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + ALL_SLAVES_PATH, false) == null)
                        zooKeeper.create(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + ALL_SLAVES_PATH,
                                "AllSlaves".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } else {
                    zooKeeper.create(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH, "SlavesRoot".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }

                if (zooKeeper.exists(APPLICATION_ROOT_PATH + MASTERS_ROOT_PATH, false) == null)
                    zooKeeper.create(APPLICATION_ROOT_PATH + MASTERS_ROOT_PATH,
                            "MastersRootPath".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            } else {
                zooKeeper.create(APPLICATION_ROOT_PATH, "MapReduceRoot".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static List<String> getIdleSlavePaths() {
        try {
            return zooKeeper.getChildren(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH, false);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static List<String> getBusySlavePaths() {
        try {
            return zooKeeper.getChildren(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + BUSY_SLAVES_PATH, false);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static List<String> getAllSlavePaths() {
        try {
            return zooKeeper.getChildren(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + BUSY_SLAVES_PATH, false);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static List<String> getMasterPaths() {
        try {
            return zooKeeper.getChildren(APPLICATION_ROOT_PATH + MASTERS_ROOT_PATH, false);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Node getNodeFrom(String nodePath) {
        try {
            return Node.deserialize(zooKeeper.getData(nodePath, false, null));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void changeSlaveState(String node, Node.Status status) {
        try {
            switch (status) {
                case STARTUP:
                    if(zooKeeper.exists(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + ALL_SLAVES_PATH + node, false)
                            == null)
                        zooKeeper.create(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + ALL_SLAVES_PATH + node,
                                node.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                    zooKeeper.create(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH + node,
                            node.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    break;

                case IDLE:
                    zooKeeper.delete(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + BUSY_SLAVES_PATH + node, -1);
                    zooKeeper.create(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH + node,
                            node.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    break;

                case BUSY:
                    zooKeeper.delete(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH + node, -1);
                    zooKeeper.create(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + BUSY_SLAVES_PATH + node,
                            node.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    break;

                case DEAD:
                    if(zooKeeper.exists(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + BUSY_SLAVES_PATH + node, false)
                            == null)
                        zooKeeper.delete(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + BUSY_SLAVES_PATH + node, -1);

                    else if(zooKeeper.exists(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH + node, false)
                            == null)
                        zooKeeper.delete(APPLICATION_ROOT_PATH + SLAVES_ROOT_PATH + IDLE_SLAVES_PATH + node, -1);
                    break;
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public static void dispatchTask(Task task, String nodeID)  {
    }

    public static ResourceManager getResourceManagerInstance() {
        if (resourceManagerInstance == null) {
            resourceManagerInstance = new ResourceManager();
        }
        return resourceManagerInstance;
    }

    private class EventWatcher implements Watcher   {

        @Override
        public void process(WatchedEvent event) {
        }
    }
}
