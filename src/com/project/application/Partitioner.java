package com.project.application;

/**
 * Created by alok on 4/23/15 in ProjectMapReduce
 */
public interface Partitioner {
    public Integer getPartitionID(String key);
}
