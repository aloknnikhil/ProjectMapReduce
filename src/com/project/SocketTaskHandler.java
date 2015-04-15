package com.project;

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
    private static final int MAX_RETRY_COUNT = 3;
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
            connectTo(entry.getKey());
        }
    }

    public void setupSocketListener() {
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
                    while (!Thread.currentThread().interrupted()) {
                        socket = serverSocket.accept();
                        socket.setSendBufferSize(64 * 1024);
                        socket.setReceiveBufferSize(64 * 1024);
                        masterSocket = new ObjectOutputStream(
                                new BufferedOutputStream(socket.getOutputStream()));
                        serverInstance = new Thread(new ServerProcess(socket));
                        serverInstance.start();
                    }
                    serverSocket.close();

                } catch (IOException e) {
                    System.out.println("Error! Something went wrong. Please restart the process");
                }
            }
        };

        new Thread(dispatcherRunnable).start();
    }

    public static void dispatchTask(final Task task) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ObjectOutputStream nodeSocket;
                if(MapRSession.getInstance().getActiveNode().getType() == Node.Type.MASTER)
                    nodeSocket = getInstance().connectedSlaves.get(task.getExecutorID());
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
                new Thread(new ServerProcess(socket)).start();
                redo = false;
            } catch (java.net.SocketException e) {
                try {
                    Thread.sleep(2000);
                    redo = true;
                } catch (InterruptedException e1) {
                    System.out.println("Connect process interrupted. Exiting process.");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } while (redo);
        getInstance().pendingHeartBeats.put(slaveID, 0);
        startHeartBeat(slaveID);
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
                    Thread.sleep(5000);
                    if (getInstance().pendingHeartBeats.get(slaveID) > MAX_RETRY_COUNT) {
                        getInstance().offlineSlaves.add(slaveID);
                        MapRSession.getInstance().getActiveNode().getJobTracker().rescheduleTasksFrom(slaveID);
                        Thread.currentThread().interrupt();
                    }

                    task = new Task();
                    task.setType(Task.Type.HEARTBEAT);
                    task.setStatus(Task.Status.INITIALIZED);
                    task.setExecutorID(slaveID);
                    getInstance().pendingHeartBeats.put(slaveID, getInstance().pendingHeartBeats.get(slaveID) + 1);
                    if(writeTaskToSocket(getInstance().connectedSlaves.get(slaveID), task))
                        System.out.println("Beat for " + slaveID);
                    else    {
                        getInstance().pendingHeartBeats.put(slaveID, MAX_RETRY_COUNT + 1);
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("Slave " + slaveID + " has died :(");
            }
        }
    }

    private static class ServerProcess implements Runnable {

        Socket socket;
        ObjectInputStream objectInputStream;
        BufferedInputStream bufferedInputStream;
        Task task;

        private ServerProcess(Socket socket) throws IOException {
            this.socket = socket;
        }

        @Override
        public void run() {

            try {
                bufferedInputStream = new BufferedInputStream(socket.getInputStream());
                objectInputStream = new ObjectInputStream(bufferedInputStream);
                while (true) {
                    task = (Task) objectInputStream.readUnshared();
                    switch (MapRSession.getInstance().getActiveNode().getType()) {
                        case MASTER:
                            if(task.getType() == Task.Type.ACK) {
                                getInstance().pendingHeartBeats.put(task.getExecutorID(), 0);
                            } else {
                                switch (task.getStatus()) {
                                    case COMPLETE:
                                        new Thread(new Runnable() {
                                            @Override
                                            public void run() {
                                                MapRSession.getInstance().getActiveNode().getJobTracker().collectTaskOutput(task);
                                                MapRSession.getInstance().getActiveNode().getJobTracker().markTaskComplete(task);
                                            }
                                        }).start();
                                        break;
                                }
                            }
                            break;

                        case SLAVE:
                            switch (task.getStatus()) {
                                case INITIALIZED:
                                    if (task.getType() == Task.Type.MAP) {
                                        task.setStatus(Task.Status.RUNNING);
                                        SocketTaskHandler.modifyTask(task);
                                        new Thread(new Runnable() {
                                            @Override
                                            public void run() {
                                                MapRSession.getInstance().getActiveNode().getTaskTracker().runMap(task);
                                            }
                                        }).start();
                                    } else if (task.getType() == Task.Type.REDUCE) {
                                        task.setStatus(Task.Status.RUNNING);
                                        SocketTaskHandler.modifyTask(task);
                                        new Thread(new Runnable() {
                                            @Override
                                            public void run() {
                                                MapRSession.getInstance().getActiveNode().getTaskTracker().runReduce(task);
                                            }
                                        }).start();
                                    } else if (task.getType() == Task.Type.HEARTBEAT)   {
                                        task.setType(Task.Type.ACK);
                                        modifyTask(task);
                                    }
                                    break;
                            }
                    }
                }
            } catch (IOException e) {
                System.out.println("A slave has failed to communicate");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
