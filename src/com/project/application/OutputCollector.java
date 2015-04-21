package com.project.application;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public interface OutputCollector {
    public void collect(String key, Integer value);
}
