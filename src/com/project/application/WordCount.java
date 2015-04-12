package com.project.application;

import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class WordCount implements Mapper, Reducer, Serializable {
    @Override
    public void map(File key, OutputCollector outputCollector) {
        String temp;
        StringTokenizer stringTokenizer;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(key));
            while ((temp = bufferedReader.readLine()) != null)  {
                stringTokenizer = new StringTokenizer(temp, " ");
                while (stringTokenizer.hasMoreTokens()) {
                    outputCollector.collect(new Pair<>(stringTokenizer.nextToken(), 1));
                }
            }
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void reduce(String key, Iterator<Integer> values, OutputCollector outputCollector) {
        int result = 0;
        while (values.hasNext())    {
            result += values.next();
        }

        outputCollector.collect(new Pair<String, Integer>(key, result));
    }
}
