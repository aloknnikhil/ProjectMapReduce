package com.project.utils;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/**
 * Created by alok on 4/13/15 in ProjectMapReduce
 */
public class DataSerializer implements ZkSerializer {
    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        return (byte[]) data;
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return bytes;
    }
}
