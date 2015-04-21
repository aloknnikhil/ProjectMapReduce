package com.project.application;

import java.io.File;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public interface Mapper {
    public void map(File key, OutputCollector outputCollector);
}
