package com.project.storage;

import com.project.utils.Node;

import java.io.File;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class FileSystem {

    private static FileSystem fileSystemInstance;
    private Node masterNode;

    private FileSystem()    {
        //Placeholder to enforce Singleton Paradigm
    }

    public static void setFileSystemManager(Node masterNode)    {

        getFileSystemInstance().masterNode = masterNode;
    }

    public static void startNameNodeService()    {

    }

    private static FileSystem getFileSystemInstance() {

        if(fileSystemInstance == null) {
            fileSystemInstance = new FileSystem();
        }

        return fileSystemInstance;
    }

    public static String copyFromLocalFile(File localFile)  {
        //TODO Return path in Cassandra
        return localFile.getAbsolutePath();
    }

    public static File copyFromRemotePath(String remoteDataPath)  {
        //TODO Return file from data in Cassandra
        return new File("output");
    }
}
