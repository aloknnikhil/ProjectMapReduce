package com.project;

import com.project.utils.LogFile;
import com.project.utils.Node;
import com.project.utils.Task;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class SocketTaskHandler {

    private static SocketTaskHandler resourceManagerInstance;
    private HashMap<Integer, ObjectOutputStream> connectedSlaves;
    private HashMap<Integer, Integer> pendingHeartBeats;
    private static final int MAX_RETRY_COUNT = 10;
    public List<Integer> offlineSlaves;
    private ObjectOutputStream masterSocket;
    private Runnable dispatcherRunnable;

    private SocketTaskHandler() {
        connectedSlaves = new HashMap<>();
        offlineSlaves = new ArrayList<>();
        pendingHeartBeats = new HashMap<>();
    }

    public void connectToSlaves() {
        for (Map.Entry<Integer, String> entry : ResourceManager.slaveAddresses.entrySet()) {
            LogFile.writeToLog("Waiting to connect to slave " + entry.getKey() + " at address " + entry.getValue());
            connectTo(entry.getKey());
        }
    }

    public void setupSocketListener() {
        LogFile.writeToLog("Waiting for connection request from Master");
        dispatcherRunnable = new Runnable() {

            Socket socket;
            Thread serverInstance;

            @Override
            public void run() {
                try {
                    String hostAddress = ResourceManager.slaveAddresses.get(MapRSession.getInstance()
                            .getActiveNode().getNodeID());
                    StringTokenizer stringTokenizer = new StringTokenizer(hostAddress, ":");
                    stringTokenizer.nextToken();
                    int port = Integer.parseInt(stringTokenizer.nextToken());

                    ServerSocket serverSocket = new ServerSocket(port);
                    while (!Thread.interrupted()) {
                        socket = serverSocket.accept();
                        socket.setSendBufferSize(64 * 1024);
                        socket.setReceiveBufferSize(64 * 1024);
                        masterSocket = new ObjectOutputStream(
                                new BufferedOutputStream(socket.getOutputStream()));
                        serverInstance = new Thread(new ServerProcess(socket, 0));
                        serverInstance.start();
                        LogFile.writeToLog("Connected to master successfully");
                        LogFile.writeToLog("Terminating connection listener. We already have one master.");
                        break;
                    }
                    serverSocket.close();

                } catch (IOException e) {
                    LogFile.writeToLog("Error! Something went wrong. Shutting down process");
                }
            }
        };

        new Thread(dispatcherRunnable).start();
    }

    public static void dispatchTask(final Task task) {

        if(task.getType() != Task.Type.ACK && task.getType() != Task.Type.HEARTBEAT)
            LogFile.writeToLog("Sending task to slave " + task.getCurrentExecutorID());

        new Thread(new Runnable() {
            @Override
            public void run() {
                ObjectOutputStream nodeSocket;
                if(MapRSession.getInstance().getActiveNode().getType() == Node.Type.MASTER)
                    nodeSocket = getInstance().connectedSlaves.get(task.getCurrentExecutorID());
                else
                    nodeSocket = getInstance().masterSocket;
                writeTaskToSocket(nodeSocket, task);
            }
        }).start();
    }

    public static void modifyTask(Task task) {
        dispatchTask(task);
    }

    public static SocketTaskHandler getInstance() {
        if (resourceManagerInstance == null) {
            resourceManagerInstance = new SocketTaskHandler();
        }
        return resourceManagerInstance;
    }

    public static void connectTo(Integer slaveID) {
        Socket socket;

        StringTokenizer stringTokenizer = new StringTokenizer(ResourceManager.slaveAddresses.get(slaveID), ":");
        String hostname = stringTokenizer.nextToken();
        String port = stringTokenizer.nextToken();
        boolean redo = true;

        do {
            try {
                socket = new Socket(hostname, Integer.valueOf(port));
                socket.setSendBufferSize(64 * 1024);
                socket.setReceiveBufferSize(64 * 1024);
                getInstance().connectedSlaves.put(slaveID, new ObjectOutputStream(
                        new BufferedOutputStream(socket.getOutputStream())));
                new Thread(new ServerProcess(socket, slaveID)).start();
                redo = false;
            } catch (java.net.SocketException e) {
                try {
                    Thread.sleep(2000);
                    redo = true;
                } catch (InterruptedException e1) {
                    LogFile.writeToLog("Connect process interrupted. Exiting process.");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } while (redo);
        getInstance().pendingHeartBeats.put(slaveID, 0);
        LogFile.writeToLog("Connected to slave " + slaveID);
        startHeartBeat(slaveID);
        LogFile.writeToLog("Started HeartBeat instance for slave " + slaveID);
    }

    private static void startHeartBeat(Integer slaveID)    {
        new Thread(new HeartBeatRunnable(slaveID)).start();
    }

    public static boolean writeTaskToSocket(ObjectOutputStream socket, Task task) {

        try {
            socket.writeUnshared(task);
            socket.flush();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static class HeartBeatRunnable implements Runnable  {

        Integer slaveID;

        private HeartBeatRunnable(Integer slaveID)  {
            this.slaveID = slaveID;
        }

        @Override
        public void run() {

            Task task;

            try {
                while (true) {
                    Thread.sleep(2048);
                    if (getInstance().pendingHeartBeats.get(slaveID) > MAX_RETRY_COUNT) {
                        getInstance().offlineSlaves.add(slaveID);
                        MapRSession.getInstance().getActiveNode().getJobTracker().acknowledgeFailure();
                        Thread.currentThread().interrupt();
                    }

                    task = new Task();
                    task.setType(Task.Type.HEARTBEAT);
                    task.setStatus(Task.Status.INITIALIZED);
                    task.setCurrentExecutorID(slaveID);
                    getInstance().pendingHeartBeats.put(slaveID, getInstance().pendingHeartBeats.get(slaveID) + 1);
                    if(!writeTaskToSocket(getInstance().connectedSlaves.get(slaveID), task))  {
                        getInstance().pendingHeartBeats.put(slaveID, MAX_RETRY_COUNT + 1);
                    }
                }
            } catch (InterruptedException e) {
                LogFile.writeToLog("Slave " + slaveID + " has died");
            }
        }
    }

    private static class ServerProcess implements Runnable {

        Socket socket;
        Integer slaveID;
        ObjectInputStream objectInputStream;
        BufferedInputStream bufferedInputStream;
        Task task = new Task();

        private ServerProcess(Socket socket, Integer slaveID) throws IOException {
            this.socket = socket;
            this.slaveID = slaveID;
        }

        @Override
        public void run() {

            try {
                bufferedInputStream = new BufferedInputStream(socket.getInputStream());
                objectInputStream = new ObjectInputStream(bufferedInputStream);
                while (true) {
                    switch (MapRSession.getInstance().getActiveNode().getType()) {
                        case MASTER:
                            synchronized (task) {
                                task = (Task) objectInputStream.readUnshared();
                                if (task.getType() == Task.Type.ACK) {
                                    getInstance().pendingHeartBeats.put(task.getCurrentExecutorID(), 0);
                                } else {
                                    switch (task.getStatus()) {
                                        case END:
                                        case COMPLETE:
                                            new Thread(new TaskOutputProcessor(task)).start();
                                            break;
                                    }
                                }
                            }
                            break;

                        case SLAVE:
                            try {
                                task = (Task) objectInputStream.readUnshared();
                            } catch (Exception e)   {
                                continue;
                            }
                            switch (task.getStatus()) {
                                case INITIALIZED:
                                    if (task.getType() == Task.Type.MAP) {
                                        task.setStatus(Task.Status.RUNNING);
                                        SocketTaskHandler.modifyTask(task);
                                        new Thread(new Runnable() {
                                            @Override
                                            public void run() {
                                                MapRSession.getInstance().getActiveNode().getTaskTracker()
                                                        .runMap(task);
                                            }
                                        }).start();
                                    } else if (task.getType() == Task.Type.REDUCE) {
                                        task.setStatus(Task.Status.RUNNING);
                                        SocketTaskHandler.modifyTask(task);
                                        new Thread(new Runnable() {
                                            @Override
                                            public void run() {
                                                MapRSession.getInstance().getActiveNode().getTaskTracker()
                                                        .runReduce(task);
                                            }
                                        }).start();
                                    } else if (task.getType() == Task.Type.HEARTBEAT) {
                                        task.setType(Task.Type.ACK);
                                        modifyTask(task);
                                    }
                                    break;

                                case COMPLETE:
                                    new Thread(new Runnable() {
                                        @Override
                                        public void run() {
                                            MapRSession.getInstance().getActiveNode().getTaskTracker()
                                                    .getPhaseOutput(task);
                                        }
                                    }).start();
                            }
                    }
                }
            } catch (IOException e) {
                if(MapRSession.getInstance().getActiveNode().getType() == Node.Type.MASTER)
                    LogFile.writeToLog("Slave " + slaveID + " failed to communicate");
                else
                    LogFile.writeToLog("Connection to master lost");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private static class TaskOutputProcessor implements Runnable {

        Task task;

        private TaskOutputProcessor(Task task) {
            this.task = task;
        }

        @Override
        public void run() {
            MapRSession.getInstance().getActiveNode().getJobTracker().collectTaskOutput(task);
            if(task.getStatus() == Task.Status.COMPLETE)
                MapRSession.getInstance().getActiveNode().getJobTracker().markTaskComplete(task);
        }
    }

}