package com.project;

import com.project.application.Partitioner;

import java.io.Serializable;

/**
 * Created by alok on 4/23/15 in ProjectMapReduce
 */
public class PartitionManager implements Partitioner, Serializable {
    @Override
    public Integer getPartitionID(String key) {
        char ch[] = key.toCharArray();

        int i, sum;
        for (sum=0, i=0; i < key.length(); i++)
            sum += ch[i];
        return (sum % ConfigurationManager.slaveAddresses.size());
    }
}
