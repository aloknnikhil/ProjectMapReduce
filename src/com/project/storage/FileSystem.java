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
import com.project.MapRSession;
import com.project.utils.LogFile;
import com.project.utils.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class FileSystem {

    private static FileSystem fileSystemInstance;
    private AstyanaxContext<Keyspace> context;
    private Keyspace keyspace;
    private CassandraChunkedStorageProvider chunkedStorageProvider;
    public static ColumnFamily<String, String> CF_CHUNK =
            ColumnFamily.newColumnFamily("cfchunk", StringSerializer.get(), StringSerializer.get());

    private FileSystem() {
        configureFileSystem();
    }

    private void configureFileSystem() {
        connectToBackStore();
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
        File remoteFile = new File(MapRSession.getRootDir(), remoteDataPath.substring(remoteDataPath.lastIndexOf("/") + 1));
        try {
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
                .forCluster("Thrust Cluster")
                .forKeyspace("BirdCount")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                        .setTargetCassandraVersion("2.1")
                        .setCqlVersion("3.0.0"))
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MapRConnectionPool")
                        .setPort(9160)
                        .setMaxConnsPerHost(1)
                        .setConnectTimeout(10000)
                        .setSeeds(MapRSession.getInstance().getCassandraHost()))
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
    }

    private void disconnectFromBackStore() {
        context.shutdown();
    }
}
