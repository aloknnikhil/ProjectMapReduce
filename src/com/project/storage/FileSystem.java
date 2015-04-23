package com.project.storage;

import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.cql.JavaDriverConfigBuilder;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.storage.CassandraChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.project.MapRSession;
import com.project.utils.LogFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

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
    private HashMap<byte[], String> cacheRemoteRefs;
    private HashMap<byte[], String> cacheLocalRefs;
    private MessageDigest messageDigest;

    private FileSystem() {
        configureFileSystem();
    }

    private void configureFileSystem() {
        connectToBackStore();
        cacheRemoteRefs = new HashMap<>();
        cacheLocalRefs = new HashMap<>();
    }

    private static FileSystem getInstance() {

        if (fileSystemInstance == null) {
            fileSystemInstance = new FileSystem();
        }

        return fileSystemInstance;
    }

    public static String copyFromLocalFile(File localFile) {
        try {
            byte[] fileContents = IOUtils.toByteArray(new FileInputStream(localFile));
            getInstance().messageDigest = MessageDigest.getInstance("MD5");
            getInstance().messageDigest.update(fileContents);
            byte[] hashContents = getInstance().messageDigest.digest();

            synchronized (getInstance().cacheRemoteRefs) {
                if (getInstance().cacheRemoteRefs.containsKey(hashContents)) {
                    return getInstance().cacheRemoteRefs.get(hashContents);
                }
            }

            ByteArrayInputStream in = new ByteArrayInputStream(fileContents);
            ChunkedStorage.newWriter(getInstance().chunkedStorageProvider,
                    localFile.getAbsolutePath(), in)
                    .withChunkSize(16384)
                    .call();
            getInstance().cacheRemoteRefs.put(hashContents, localFile.getAbsolutePath());
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
                .withHostSupplier(HostSupplier)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                        .setDiscoveryDelayInSeconds(60000)
                        .setDefaultReadConsistencyLevel(ConsistencyLevel.CL_ONE)
                        .setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_ONE)
                        .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                        .setTargetCassandraVersion("2.1")
                        .setCqlVersion("3.0.0"))
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MapRConnectionPool"))
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
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

    final Supplier<List<Host>> HostSupplier = new Supplier<List<Host>>() {

        @Override
        public List<Host> get() {
            Host host = new Host(MapRSession.getInstance().getCassandraSeeds(), 9160);
            return Collections.singletonList(host);
        }
    };

    public static void close()  {
        getInstance().disconnectFromBackStore();
    }

    private void disconnectFromBackStore() {
        context.shutdown();
    }
}