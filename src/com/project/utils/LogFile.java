package com.project.utils;

import com.project.MapRSession;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

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
            logFileInstance = new LogFile(MapRSession.getRootDir(), "logfile");

        try {
            logFileInstance.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return logFileInstance;
    }

    public static void writeToLog(String message)   {
        PrintWriter printWriter;
        message = getLogFileInstance().getTime() + message;
        try {
            printWriter = new PrintWriter(new FileWriter(getLogFileInstance(), MapRSession.flag));
            if(!MapRSession.flag)
                MapRSession.flag = true;

            printWriter.println(message);
            System.out.println(message);
            printWriter.flush();
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized String getTime() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM/dd hh:mm:ss");
        return "[" + simpleDateFormat.format(new Date()) + "] ";

    }
}
