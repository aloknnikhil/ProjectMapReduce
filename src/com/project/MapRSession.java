package com.project;

import com.project.mapr.JobTracker;
import com.project.utils.Input;
import com.project.utils.Node;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alok on 4/11/15 in ProjectMapReduce
 */
public class MapRSession {

    private static MapRSession mapRSessionInstance;
    private File sessionDir;
    private Input input;
    public static boolean flag = false;

    public MapRSession()   {
        configureSession();
    }

    private void configureSession() {
        sessionDir = new File("session" + System.currentTimeMillis());
        sessionDir.mkdir();
        input = new Input(new File("input"));
    }

    public static File getRootDir() {
        return mapRSessionInstance.sessionDir;
    }

    private static MapRSession getMapRSessionInstance() {
        if(mapRSessionInstance == null)
            mapRSessionInstance = new MapRSession();

        return mapRSessionInstance;
    }

    public static void startJobTracker()  {
        List<Node> slaveNodes = new ArrayList<>();
        while(ResourceManager.getIdleSlavePaths().size() != ResourceManager.getAllSlavePaths().size())  {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (String slavePath : ResourceManager.getIdleSlavePaths())    {
            slaveNodes.add(ResourceManager.getNodeFrom(slavePath));
        }
        ResourceManager.jobTracker = new JobTracker(mapRSessionInstance.input, slaveNodes);
        ResourceManager.jobTracker.startScheduler();
    }

    public static void startTaskTracker() {

    }

    public static void startDataNode()    {

    }

    public static void exit(int status) {
        System.exit(status);
    }

}
