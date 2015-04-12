package com.project.storage;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.recipes.storage.CassandraChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ObjectMetadata;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.project.utils.Node;
import org.apache.cassandra.hadoop.pig.CassandraStorage;

import java.io.File;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class FileSystem {

    private static FileSystem fileSystemInstance;
    private Node masterNode;
    private ChunkedStorageProvider provider;


    private FileSystem()    {
        //Placeholder to enforce Singleton Paradigm
    }

    private void configureFileSystem()  {

        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forCluster("ClusterName")
                .forKeyspace("KeyspaceName")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                                .setPort(9160)
                                .setMaxConnsPerHost(1)
                                .setSeeds("127.0.0.1:9160")
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        Keyspace keyspace = context.getClient();

        provider = new CassandraChunkedStorageProvider(
                keyspace,
                "data_column_family_name");

        ObjectMetadata meta = ChunkedStorage.newWriter(provider, objName, someInputStream)
                .withChunkSize(0x1000)    // Optional chunk size to override
                        // the default for this provider
                .withConcurrencyLevel(8)  // Optional. Upload chunks in 8 threads
                .withTtl(60)              // Optional TTL for the entire object
                .call();
    }

    public static void setFileSystemManager(Node masterNode)    {

        getFileSystemInstance().masterNode = masterNode;
    }

    public static void startNameNodeService()    {

    }

    private static FileSystem getFileSystemInstance() {

        if(fileSystemInstance == null) {
            fileSystemInstance = new FileSystem();
        }

        return fileSystemInstance;
    }

    public static String copyFromLocalFile(File localFile)  {
        //TODO Return path in Cassandra
        return localFile.getAbsolutePath();
    }

    public static File copyFromRemotePath(String remoteDataPath)  {
        //TODO Return file from data in Cassandra
        return new File("output");
    }
}
