package com.linbit.linstor.dbcp;

import com.linbit.linstor.dbcp.migration.LinstorMigration;
import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.DATABASE_SCHEMA_NAME;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.flywaydb.core.Flyway;

public class GenerateSql {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "linstor");
        props.setProperty("password", "linstor");
        ConnectionFactory connFactory = new DriverManagerConnectionFactory("jdbc:h2:mem:testDB", props);
        PoolableConnectionFactory poolConnFactory = new PoolableConnectionFactory(connFactory, null);

        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMinIdle(10);
        poolConfig.setMaxIdle(100);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setFairness(true);
        GenericObjectPool<PoolableConnection> connPool = new GenericObjectPool<>(poolConnFactory, poolConfig);

        poolConnFactory.setPool(connPool);
        poolConnFactory.setValidationQueryTimeout(60000);
        poolConnFactory.setMaxOpenPrepatedStatements(100);
        poolConnFactory.setMaxConnLifetimeMillis(3600000);
        poolConnFactory.setDefaultTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        PoolingDataSource<PoolableConnection> dataSource = new PoolingDataSource<>(connPool);

        Flyway flyway = Flyway.configure()
            .schemas(DATABASE_SCHEMA_NAME)
            .dataSource(dataSource)
            .table("FLYWAY_SCHEMA_HISTORY")
            // When migrations are added in branches they can be applied in different orders
            .outOfOrder(true)
            // Pass the DB type to the migrations
            .placeholders(ImmutableMap.of(LinstorMigration.PLACEHOLDER_KEY_DB_TYPE, "h2"))
            .locations(LinstorMigration.class.getPackage().getName())
            .load();

        flyway.baseline();
        flyway.migrate();

        String javaClazz = DatabaseConstantsGenerator.generateSqlConstants(dataSource.getConnection());

        ProcessBuilder pb = new ProcessBuilder("git", "rev-parse", "--show-toplevel");
        Process p = pb.start();
        int rc = p.waitFor();
        if (rc != 0)
        {
            throw new RuntimeException("git base path checking failed:" + pb.toString());
        }
        final String gitRoot = new BufferedReader(
            new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
        System.out.println(gitRoot);
        Path path = Paths.get(
            gitRoot, "controller", "src", "main", "java",
            DatabaseConstantsGenerator.DFLT_PACKAGE.replaceAll("[.]", "/"),
            DatabaseConstantsGenerator.DFLT_CLAZZ_NAME + ".java"
        );
        Files.write(path, javaClazz.getBytes());

        // Files.write(
        // path.getParent().resolve("dump.json"), SqlDump.getDump(dataSource.getConnection()).serializeJson().getBytes()
        // );

        dataSource.close();
    }

}
