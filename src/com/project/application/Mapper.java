package com.project.application;

import javafx.util.Pair;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public interface Mapper {
    public Pair<String, String> map(String key, String value);
}
