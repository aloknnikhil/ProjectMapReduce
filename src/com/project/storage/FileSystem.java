package com.project.storage;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
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
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.commons.io.IOUtils;

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
        try {
            ChunkedStorage.newWriter(getInstance().chunkedStorageProvider,
                    localFile.getAbsolutePath(), new FileInputStream(localFile))
                    .withChunkSize(100)
                    .call();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return localFile.getAbsolutePath();
    }

    public static File copyFromRemotePath(String remoteDataPath) {
        File remoteFile = new File(remoteDataPath + "remote");
        String temp;
        try {
            ObjectMetadata metadata = ChunkedStorage.newInfoReader(getInstance().
                    chunkedStorageProvider, remoteDataPath).call();
            FileOutputStream outputStream = new FileOutputStream(remoteFile);
            ChunkedStorage.newReader(getInstance().chunkedStorageProvider, remoteDataPath, outputStream)
                    .withBatchSize(11)
                    .withConcurrencyLevel(3)
                    .call();
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
            keyspace.createKeyspaceIfNotExists(ImmutableMap.<String, Object>builder()
                            .put("strategy_options", ImmutableMap.<String, Object>builder()
                                    .put("replication_factor", "1")
                                    .build())
                            .put("strategy_class", "SimpleStrategy")
                            .build()
            );

            keyspace.createColumnFamily(CF_CHUNK, null);

        } catch (BadRequestException e) {

        } catch (ConnectionException e) {
            e.printStackTrace();
        }

        chunkedStorageProvider = new CassandraChunkedStorageProvider(keyspace, CF_CHUNK);
        copyFromRemotePath(copyFromLocalFile(new File("testFile")));
    }

    private void disconnectFromBackStore() {
        context.shutdown();
    }
}
