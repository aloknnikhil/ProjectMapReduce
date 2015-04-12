package com.project.application;

import javafx.util.Pair;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public interface OutputCollector {
    public void collect(Pair<String, Integer> keyValuePair);
}
