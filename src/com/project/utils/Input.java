package com.project.utils;

import java.io.File;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class Input {

    public enum Type    {
        LOCAL,
        REMOTE
    }

    private int fileCount;
    private File localFile;
    private String remoteDataPath;
    private Type type;

    public Input(File inputFile)  {
        fileCount = countFiles(inputFile);
        localFile = inputFile;
        type = Type.LOCAL;
    }

    public Input(String remoteDataPath)  {
        fileCount = 0;
        localFile = null;
        this.remoteDataPath = remoteDataPath;
        type = Type.REMOTE;
    }

    public static int countFiles(File directory) {
        int count = 0;
        for(File file : directory.listFiles()) {
            if(file.isDirectory()) {
                count += countFiles(file);
            }
            count++;
        }
        return count;
    }

    public File getLocalFile() {
        return localFile;
    }

    public int getFileCount() {
        return fileCount;
    }

    public Type getType() {
        return type;
    }
}
