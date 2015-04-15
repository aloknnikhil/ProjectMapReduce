package com.project.utils;

import java.io.File;
import java.io.Serializable;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class Input implements Serializable {

    public enum Type    {
        LOCAL,
        REMOTE
    }

    private File localFile;
    private String remoteDataPath;
    private Type type;

    public Input(File inputFile)  {
        localFile = inputFile;
        type = Type.LOCAL;
    }

    public Input(String remoteDataPath)  {
        localFile = null;
        this.remoteDataPath = remoteDataPath;
        type = Type.REMOTE;
    }

    public File getLocalFile() {
        return localFile;
    }

    public Type getType() {
        return type;
    }

    public String getRemoteDataPath() {
        return remoteDataPath;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setRemoteDataPath(String remoteDataPath) {
        this.remoteDataPath = remoteDataPath;
    }

    public void setLocalFile(File localFile) {
        this.localFile = localFile;
    }
}
