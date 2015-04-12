package com.project.application;

import javafx.util.Pair;

import java.util.Iterator;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public interface Reducer {
    public void reduce(String key, Iterator<Integer> values, OutputCollector outputCollector);
}
