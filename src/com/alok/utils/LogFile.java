package com.alok.utils;

import com.alok.Session;

import java.io.*;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class LogFile extends File {

    private static LogFile logFileInstance;

    private LogFile(File parent, String child) {
        super(parent, child);
    }

    private static LogFile getLogFileInstance() {
        if(logFileInstance == null)
            logFileInstance = new LogFile(Session.getRootDir(), "logfile");

        try {
            logFileInstance.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return logFileInstance;
    }

    public static void writeToLog(String message)   {
        PrintWriter printWriter;
        try {
            printWriter = new PrintWriter(new FileWriter(getLogFileInstance(), Session.flag));
            if(!Session.flag)
                Session.flag = true;

            printWriter.println(message);
            printWriter.flush();
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
