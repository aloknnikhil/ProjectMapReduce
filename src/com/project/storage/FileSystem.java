package com.project.storage;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.recipes.storage.CassandraChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ObjectMetadata;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.project.utils.LogFile;
import com.project.utils.Node;

import java.io.*;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class FileSystem {

    private static FileSystem fileSystemInstance;
    private Node masterNode;
    private String backStoreAddress = "127.0.0.1";
    private AstyanaxContext<Keyspace> context;
    private Keyspace keyspace;
    private CassandraChunkedStorageProvider chunkedStorageProvider;
    public static ColumnFamily<String, String> CF_CHUNK =
            ColumnFamily.newColumnFamily("cfchunk", StringSerializer.get(), StringSerializer.get());

    public FileSystem() {
        //Placeholder to enforce Singleton Paradigm
    }

    public static void main(String[] args) {
        fileSystemInstance = new FileSystem();
        fileSystemInstance.configureFileSystem();
    }

    public void configureFileSystem() {
        connectToBackStore();
        disconnectFromBackStore();
    }

    public static void setFileSystemManager(Node masterNode) {

        getInstance().masterNode = masterNode;
    }

    public static void startNameNodeService() {

    }

    private static FileSystem getInstance() {

        if (fileSystemInstance == null) {
            fileSystemInstance = new FileSystem();
        }

        return fileSystemInstance;
    }

    public static String copyFromLocalFile(File localFile) {
        String temp;
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serialize(localFile));
        try {
            ChunkedStorage.newWriter(getInstance().chunkedStorageProvider,
                    localFile.getAbsolutePath(), byteArrayInputStream)
                    .withChunkSize(100)
                    .call();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return localFile.getAbsolutePath();
    }

    public static File copyFromRemotePath(String remoteDataPath) {
        try {
            ObjectMetadata metadata = ChunkedStorage.newInfoReader(getInstance().
                    chunkedStorageProvider, remoteDataPath).call();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(metadata.getObjectSize().intValue());
            ChunkedStorage.newReader(getInstance().chunkedStorageProvider, remoteDataPath, outputStream)
                    .withBatchSize(11)
                    .withConcurrencyLevel(3)
                    .call();

            File remoteFile = deserialize(outputStream.toByteArray());
            LogFile.writeToLog("Remote File data: " + remoteFile.getAbsolutePath());
            return remoteFile;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private void connectToBackStore() {
        context = new AstyanaxContext.Builder()
                .forCluster("Test Cluster")
                .forKeyspace("BirdCount")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                        .setTargetCassandraVersion("2.1")
                        .setCqlVersion("3.0.0"))
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MapRConnectionPool")
                        .setPort(9160)
                        .setMaxConnsPerHost(1)
                        .setSeeds(backStoreAddress))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        LogFile.writeToLog(context.getClusterName() + " connected");
        keyspace = context.getClient();

        // Using simple strategy
        try {
            keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
                            .put("strategy_options", ImmutableMap.<String, Object>builder()
                                    .put("replication_factor", "1")
                                    .build())
                            .put("strategy_class", "SimpleStrategy")
                            .build()
            );

            keyspace.createColumnFamily(CF_CHUNK, null);

        } catch (ConnectionException e) {
            e.printStackTrace();
        }

        chunkedStorageProvider = new CassandraChunkedStorageProvider(keyspace, CF_CHUNK);
        copyFromRemotePath(copyFromLocalFile(new File("logfile")));
    }

    private void disconnectFromBackStore() {
        context.shutdown();
    }

    public static byte[] serialize(File file) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o;
        try {
            o = new ObjectOutputStream(b);
            o.writeObject(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return b.toByteArray();
    }

    public static File deserialize(byte[] bytes) {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = null;
        try {
            o = new ObjectInputStream(b);
            return (File) o.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}
