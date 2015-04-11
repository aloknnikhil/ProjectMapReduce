package com.project.utils;

import java.io.File;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class Output {

    public enum Type    {
        LOCAL,
        REMOTE
    }

    private Type type;
    private File localFile;
    private String remoteDataPath;

    public Output(File localFile)   {
        this.localFile = localFile;
        type = Type.LOCAL;
    }

    public Output(String remoteDataPath)    {
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
}
