package com.project.utils;

import java.io.File;
import java.io.Serializable;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class Output implements Serializable {

    public enum Type {
        LOCAL,
        REMOTE
    }

    private Type type;
    private File localFile;
    private String remoteDataPath;

    public Output(File localFile) {
        this.localFile = localFile;
        type = Type.LOCAL;
    }

    public Output(String remoteDataPath) {
        this.localFile = null;
        this.remoteDataPath = remoteDataPath;
        this.type = Type.REMOTE;
    }

    public Type getType() {
        return type;
    }

    public File getLocalFile() {
        return localFile;
    }

    public String getRemoteDataPath() {
        return remoteDataPath;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Output) {
            if(((Output) obj).getType() == Type.LOCAL) {
                String secondary = ((Output) obj).getLocalFile().getAbsolutePath();
                return this.localFile.getAbsolutePath().equals(secondary);
            } else {
                String secondary = ((Output) obj).getRemoteDataPath();
                return this.localFile.getAbsolutePath().equals(secondary);
            }
        }
        else
            return false;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setLocalFile(File localFile) {
        this.localFile = localFile;
    }

    public void setRemoteDataPath(String remoteDataPath) {
        this.remoteDataPath = remoteDataPath;
    }
}
