package com.project.utils;

/**
 * Created by alok on 4/14/15 in ProjectMapReduce
 */
public class Splitter {
    String key, value;
    public final static char DELIMITER = ':';

    public Splitter(String string) {
        key = string.substring(0, string.lastIndexOf(DELIMITER));
        value = string.substring(string.lastIndexOf(DELIMITER) + 1);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
