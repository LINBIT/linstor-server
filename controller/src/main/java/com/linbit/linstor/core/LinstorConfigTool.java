package com.linbit.linstor.core;

import com.linbit.GuiceConfigModule;
import com.linbit.ImplementationError;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.core.cfg.CtrlTomlConfig;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbcp.DbConnectionPoolInitializer;
import com.linbit.linstor.dbcp.DbInitializer;
import com.linbit.linstor.dbcp.etcd.DbEtcd;
import com.linbit.linstor.dbcp.etcd.DbEtcdInitializer;
import com.linbit.linstor.dbcp.k8s.crd.DbK8sCrd;
import com.linbit.linstor.dbcp.k8s.crd.DbK8sCrdInitializer;
import com.linbit.linstor.dbcp.migration.AbsMigration;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StderrErrorReporter;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;
import com.linbit.linstor.transaction.ControllerETCDTransactionMgrGenerator;
import com.linbit.linstor.transaction.ControllerK8sCrdTransactionMgrGenerator;
import com.linbit.utils.Pair;

import static com.linbit.linstor.InternalApiConsts.EXIT_CODE_CMDLINE_ERROR;
import static com.linbit.linstor.InternalApiConsts.EXIT_CODE_CONFIG_PARSE_ERROR;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.DATABASE_SCHEMA_NAME;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.moandjiezana.toml.Toml;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import picocli.CommandLine;

public class LinstorConfigTool
{
    private static @Nullable CommandLine commandLine;

    private static final String DB_USER = "linstor";
    private static final String DB_PASSWORD = "linstor";

    private static final List<String> supportedDbs = Arrays.asList("h2", "postgresql");

    private static final String DB_CFG = "[db]\n" +
        "  user = \"%s\"\n" +
        "  password = \"%s\"\n" +
        "  connection_url = \"%s\"\n";

    private static final String DEF_CTRL_TOML = "[db]\n" +
        "# user = \"linstor\"\n" +
        "# password = \"linstor\"\n" +
        "\n" +
        "## jdbc connection url\n" +
        "# connection_url = \"jdbc:h2:/var/lib/linstor/linstordb\"\n" +
        "\n" +
        "## if you use TLS with etcd/crd\n" +
        "# ca_certificate = \"ca.pem\"\n" +
        "# client_certificate = \"client.pem\"\n" +
        "# client_key_pkcs8_pem = \"client-key.pkcs8\"\n" +
        "## set client_key_password if private key has a password\n" +
        "# client_key_password = \"mysecret\"\n" +
        "\n" +
        "## for etcd\n" +
        "## do not set user field if no authentication required\n" +
        "# connection_url = \"etcd://etcdhost:2379\"\n" +
        "    [db.etcd]\n" +
        "    # prefix = \"/LINSTOR/\"\n" +
        "\n" +
        "## for k8s crd\n" +
        "# connection_url = \"k8s\"\n" +
        "    [db.k8s]\n" +
        "    ## how often to retry connecting to k8s crd\n" +
        "    # request_retries = 5\n" +
        "    ## how many rollack retries\n" +
        "    # rollback_retires = 5\n" +
        "\n" +
        "[encrypt]\n" +
        "## provide passphrase here to auto unlock Linstor encryption master passphrase\n" +
        "# passphrase = \"mysecret\"\n" +
        "\n" +
        "[http]\n" +
        "# enabled = true\n" +
        "# listen_addr = \"::\"\n" +
        "# port = 3370\n" +
        "\n" +
        "[https]\n" +
        "# enabled = false\n" +
        "# listen_addr = \"::\"\n" +
        "# port = 3371\n" +
        "\n" +
        "## keystore containing the https server certificate\n" +
        "# keystore = \"/path/to/valid/file.jks\"\n" +
        "\n" +
        "## keystore password to unlock the server certificate\n" +
        "# keystore_password = \"linstor\"\n" +
        "\n" +
        "## to only allow clients with the correct certificates\n" +
        "# truststore = \"/path/to/valid/truststore.jks\n" +
        "# truststore_password = \"password\"\n" +
        "\n" +
        "[ldap]\n" +
        "# enabled = false\n" +
        "\n" +
        "## allow_public_access: if no authorization fields are given allow users to work with the public context\n" +
        "# allow_public_access = false\n" +
        "\n" +
        "## uri: ldap uri to use e.g.: ldap://hostname\n" +
        "# uri = \"\"\n" +
        "\n" +
        "## distinguished name: {user} can be used as template for the user name\n" +
        "# dn = \"uid={user}\"\n" +
        "\n" +
        "## search base for the search_filter field\n" +
        "# search_base = \"\"\n" +
        "\n" +
        "## search_filter: ldap filter to restrict users on memberships\n" +
        "# search_filter = \"\"\n" +
        "\n" +
        "[logging]\n" +
        "# level = \"info\" # minimal log level 3rd party libs, can be trace, debug, info, warning, error\n" +
        "# linstor_level = \"info\" # minimal log level for Linstor, can be trace, debug, info, warning, error\n" +
        "\n" +
        "## path to the rest access log, if relative path it will be resolved to the linstor log directory\n" +
        "# rest_access_log_path = \"rest-access.log\"\n" +
        "\n" +
        "## rest_access_log_mode configures the way the log is archived\n" +
        "##   - \"APPEND\" will always append to the same file\n" +
        "##   - \"ROTATE_HOURLY\" will rotate the file on an hourly basis\n" +
        "##   - \"ROTATE_DAILY\"  will rotate the file on a daily basis\n" +
        "##   - \"NO_LOG\" will not write a access log file\n" +
        "# rest_access_log_mode = \"NO_LOG\"\n" +
        "\n" +
        "[webUi]\n" +
        "## path to the web ui directory\n" +
        "# directory = \"./ui\"\n";

    @CommandLine.Command(name = "linstor-config", subcommands = {
        CmdSetPlainPort.class,
        CmdSetPlainListen.class,
        CmdCreateDBTomlConfig.class,
        CmdSqlScript.class,
        CmdMigrateDatabaseConfig.class,
        CmdRunMigration.class,
        CmdAllMigrationsApplied.class
    })
    private static class LinstorConfigCmd implements Callable<Object>
    {
        @Override
        public Object call()
        {
            commandLine.usage(System.err);
            return null;
        }
    }

    @CommandLine.Command(
        name = "create-db-file",
        description = "Write a database toml configuration file to standard out."
    )
    private static class CmdCreateDBTomlConfig implements Callable<Object>
    {
        @CommandLine.Option(
            names = {"--dbtype"},
            description = "Specify the database type. ['h2', 'postgresql', 'mariadb']"
        )
        private String dbtype = "h2";

        @CommandLine.Parameters(description = "Path to the database")
        private @Nullable String dbpath;

        @Override
        public Object call() throws Exception
        {
            OutputStream os = System.out;

            DatabaseDriverInfo dbInfo = DatabaseDriverInfo.createDriverInfo(dbtype);

            if (dbInfo != null)
            {
                String ctrlToml = DEF_CTRL_TOML
                    .replace("# user = ", "user = ")
                    .replace("# password = ", "password = ")
                    .replaceFirst("# connection_url = \".*\"", "connection_url = \"" + dbInfo.jdbcUrl(dbpath) + "\"");
                os.write(ctrlToml.getBytes(StandardCharsets.UTF_8));
            }
            else
            {
                System.err.printf(
                    "Database type '%s' not supported. Use one of: '%s'%n",
                    dbtype,
                    String.join("', '", supportedDbs));
                System.exit(EXIT_CODE_CMDLINE_ERROR);
            }
            return null;
        }
    }

    @CommandLine.Command(name = "set-plain-port", description = "Set the controller plain tcp port.")
    private static class CmdSetPlainPort implements Callable<Object>
    {
        @CommandLine.Parameters(index = "0", description = "Database configuration file.")
        private File linstorTomlFile = new File("./linstor.toml");

        @CommandLine.Parameters(index = "1", description = "New Port number.")
        private int controllerPort = ApiConsts.DFLT_CTRL_PORT_PLAIN;

        @Override
        public Object call() throws Exception
        {
            try (PoolingDataSource<PoolableConnection> dataSource =
                     initConnectionProviderFromCfg(linstorTomlFile);
                 Connection con = dataSource.getConnection())
            {
                con.setSchema(DATABASE_SCHEMA_NAME);
                final String stmt = "UPDATE PROPS_CONTAINERS SET PROP_VALUE='%d' " +
                    "WHERE PROPS_INSTANCE='/CTRLCFG' AND PROP_KEY='netcom/PlainConnector/port'";
                SQLUtils.executeStatement(con, String.format(stmt, controllerPort));
                con.commit();
                System.out.println("Controller plain port set to " + controllerPort);
            }
            return null;
        }
    }

    @CommandLine.Command(name = "set-plain-listen", description = "Set the controller plain listen/bind address.")
    private static class CmdSetPlainListen implements Callable<Object>
    {
        @CommandLine.Parameters(index = "0", description = "Database configuration file.")
        private File linstorTomlFile = new File("./linstor.toml");

        @CommandLine.Parameters(index = "1", description = "new Port number.")
        private String listenAddress = "::0";

        @Override
        public Object call() throws Exception
        {
            try (PoolingDataSource<PoolableConnection> dataSource =
                     initConnectionProviderFromCfg(linstorTomlFile);
                 Connection con = dataSource.getConnection())
            {
                con.setSchema(DATABASE_SCHEMA_NAME);
                final String stmt = "UPDATE PROPS_CONTAINERS SET PROP_VALUE='%s' " +
                    "WHERE PROPS_INSTANCE='/CTRLCFG' AND PROP_KEY='netcom/PlainConnector/bindaddress'";
                SQLUtils.executeStatement(con, String.format(stmt, listenAddress));
                con.commit();

                System.out.println("Controller plain listen address set to " + listenAddress);
            }

            return null;
        }
    }

    @CommandLine.Command(name = "sql-script", description = "Runs a SQL script against the Linstor DB.")
    private static class CmdSqlScript implements Callable<Object>
    {
        @CommandLine.Parameters(index = "0", description = "Database configuration file.")
        private File linstorTomlFile = new File("./linstor.toml");

        @CommandLine.Parameters(index = "1", description = "SQL script.", arity = "0..1")
        private @Nullable File sqlFile = null;

        @Override
        public Object call() throws Exception
        {
            Reader input = sqlFile != null ? new FileReader(sqlFile) : new InputStreamReader(System.in);

            try (PoolingDataSource<PoolableConnection> dataSource =
                     initConnectionProviderFromCfg(linstorTomlFile);
                 Connection con = dataSource.getConnection())
            {
                con.setAutoCommit(false);
                try
                {
                    SQLUtils.runSql(con, new BufferedReader(input));
                    con.commit();
                }
                catch (IOException ioExc)
                {
                    System.err.println(String.format("Error reading sql script '%s'", sqlFile.toString()));
                    System.exit(EXIT_CODE_CMDLINE_ERROR);
                }
                catch (SQLException sqlExc)
                {
                    System.err.println(sqlExc.getMessage());
                    System.exit(EXIT_CODE_CMDLINE_ERROR);
                }
            }

            return null;
        }
    }

    @CommandLine.Command(name = "migrate-database-config", description = "migrate old database.cfg to linstor.toml")
    private static class CmdMigrateDatabaseConfig implements Callable<Object>
    {
        @CommandLine.Parameters(index = "0", description = "Old Database configuration file.")
        private File linstorTomlFile = new File("./datbase.cfg");

        @CommandLine.Parameters(index = "1", description = "Linstor toml configuration file.")
        private File tomlFile = new File("./linstor.toml");

        @Override
        public Object call() throws Exception
        {
            Properties dbProps = new Properties();

            if (!linstorTomlFile.exists())
            {
                System.err.println(String.format("Old database config file does not exist: '%s'.", linstorTomlFile));
                System.exit(EXIT_CODE_CONFIG_PARSE_ERROR);
            }

            try (FileInputStream fis = new FileInputStream(linstorTomlFile))
            {
                dbProps.loadFromXML(fis);
            }

            final String tomlDBEntry = String.format(
                DB_CFG,
                dbProps.getProperty("user"),
                dbProps.getProperty("password"),
                dbProps.getProperty("connection-url")
            );

            if (tomlFile.exists())
            {
                try
                {
                    Toml linstorToml = new Toml().read(tomlFile);
                    if (linstorToml.containsTable("db"))
                    {
                        System.out.println(
                            String.format("'%s' already contains [db] section, skipping operation.",
                                tomlFile.toString()
                            )
                        );
                    }
                    else
                    {
                        FileWriter fileWriter = new FileWriter(tomlFile, true);
                        fileWriter.write(tomlDBEntry);
                        fileWriter.close();

                        System.out.println(
                            String.format("Appended [db] section to '%s'.", tomlFile.toString())
                        );
                    }
                }
                catch (RuntimeException tomlExc)
                {
                    System.err.println(
                        String.format("Error parsing '%s': %s", tomlFile.toString(), tomlExc.getMessage())
                    );
                    System.exit(InternalApiConsts.EXIT_CODE_CONFIG_PARSE_ERROR);
                }
            }
            else
            {
                FileWriter fileWriter = new FileWriter(tomlFile);
                fileWriter.write(tomlDBEntry);
                fileWriter.close();

                System.out.println(
                    String.format("Successfully wrote '%s'.", tomlFile.toString())
                );
            }

            return null;
        }
    }

    @CommandLine.Command(name = "run-migration", description = "migrate configured database to the latest version")
    private static class CmdRunMigration implements Callable<Object>
    {
        @CommandLine.Option(
            names = {"--to-version"},
            description = "Specify the database version to migrate to. " +
                "Migrates to the latest version if this option is omitted"
        ) private @Nullable String toVersion;

        @CommandLine.Unmatched()
        private String[] args = new String[0];
        @Override
        public Object call() throws Exception
        {
            CtrlConfig cfg = new CtrlConfig(args);
            ErrorReporter reporter = new StderrErrorReporter("linstor-config");
            Pair<DbInitializer, ControllerDatabase> databasePair = dbFromConfig(reporter, cfg);

            if (toVersion == null)
            {
                databasePair.objA.initialize();
            }
            else
            {
                databasePair.objA.migrateTo(toVersion);
            }
            databasePair.objB.shutdown();
            return null;
        }
    }

    @CommandLine.Command(name = "all-migrations-applied", description = "checks whether the database needs migrating")
    private static class CmdAllMigrationsApplied implements Callable<Integer>
    {
        @CommandLine.Unmatched()
        private String[] args = new String[0];

        @Override
        public Integer call() throws Exception
        {
            CtrlConfig cfg = new CtrlConfig(args);
            ErrorReporter reporter = new StderrErrorReporter("linstor-config");
            Pair<DbInitializer, ControllerDatabase> databasePair = dbFromConfig(reporter, cfg);

            int result = 0;
            if (databasePair.objA.needsMigration())
            {
                System.out.println("needs migration");
                result = 1;
                System.exit(1);
            }
            else
            {
                System.out.println("no migration needed");
            }
            databasePair.objB.shutdown();

            return result;
        }
    }

    public static void main(String[] args)
    {
        commandLine = new CommandLine(new LinstorConfigCmd());

        commandLine.parseWithHandler(new CommandLine.RunLast(), args);
    }

    private static PoolingDataSource<PoolableConnection> initConnectionProvider(
        final String connUrl,
        final String user,
        final String password)
    {
        Properties dbProps = new Properties();
        if (user != null)
        {
            dbProps.setProperty("user", user);
        }
        if (password != null)
        {
            dbProps.setProperty("password", password);
        }
        ConnectionFactory connFactory = new DriverManagerConnectionFactory(
            connUrl,
            dbProps
        );
        PoolableConnectionFactory poolConnFactory = new PoolableConnectionFactory(connFactory, null);

        GenericObjectPoolConfig<PoolableConnection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setFairness(true);

        GenericObjectPool<PoolableConnection> connPool = new GenericObjectPool<>(poolConnFactory, poolConfig);

        poolConnFactory.setPool(connPool);

        return new PoolingDataSource<>(connPool);
    }

    private static PoolingDataSource<PoolableConnection> initConnectionProviderFromCfg(final File tomlFile)
    {
        PoolingDataSource<PoolableConnection> dataSource = null;
        try
        {
            CtrlTomlConfig linstorToml = new Toml().read(tomlFile).to(CtrlTomlConfig.class);

            dataSource = initConnectionProvider(
                linstorToml.getDB().getConnectionUrl(),
                linstorToml.getDB().getUser(),
                linstorToml.getDB().getPassword()
            );
        }
        catch (IllegalStateException exc)
        {
            System.err.println(
                String.format("Unable to parse configuration file: '%s': %s", tomlFile, exc.getMessage())
            );
            System.exit(EXIT_CODE_CONFIG_PARSE_ERROR);
        }
        return dataSource;
    }


    private static Pair<DbInitializer, ControllerDatabase> dbFromConfig(ErrorReporter reporter, CtrlConfig cfg) throws Exception
    {
        DatabaseDriverInfo.DatabaseType dbType = Controller.checkDatabaseConfig(reporter, cfg);
        List<Module> injModList = new ArrayList<>(Arrays.asList(
            new GuiceConfigModule(),
            new LoggingModule(reporter)
        ));

        final boolean haveFipsInit = LinStor.initializeFips(reporter);
        LinStor.loadModularCrypto(injModList, reporter, haveFipsInit);
        final Injector injector = Guice.createInjector(injModList);
        ModularCryptoProvider cryptoProvider = injector.getInstance(ModularCryptoProvider.class);
        AbsMigration.setModularCryptoProvider(cryptoProvider);
        reporter.logInfo("Cryptography provider: Using %s", cryptoProvider.getModuleIdentifier());


        // We load our DBInitializer manually here, as using dependency injection is complicated :/
        DbInitializer initializer = null;
        ControllerDatabase database = null;
        switch (dbType)
        {
            case SQL:
                database = new DbConnectionPool(cfg, reporter);
                initializer = new DbConnectionPoolInitializer(
                    reporter,
                    database,
                    cfg
                );
                break;
            case ETCD:
                EtcdUtils.linstorPrefix = cfg.getEtcdPrefix().endsWith("/") ? cfg.getEtcdPrefix() : cfg.getEtcdPrefix() + '/';
                Provider<ControllerETCDDatabase> etcdInitializerProvider = new Provider<>()
                {
                    private final DbEtcd dbEtcd = new DbEtcd(
                        reporter,
                        cfg,
                        new ControllerETCDTransactionMgrGenerator(
                            this,
                            cfg
                        )
                    );
                    @Override
                    public ControllerETCDDatabase get()
                    {
                        return dbEtcd;
                    }
                };

                database = etcdInitializerProvider.get();
                initializer = new DbEtcdInitializer(
                    reporter,
                    database,
                    cfg
                );
                break;
            case K8S_CRD:
                Provider<ControllerK8sCrdDatabase> k8sCrdProvider = new Provider<>()
                {
                    private final DbK8sCrd dbK8sCrd = new DbK8sCrd(
                        reporter,
                        cfg,
                        new ControllerK8sCrdTransactionMgrGenerator(this, cfg),
                        this
                    );
                    @Override
                    public ControllerK8sCrdDatabase get()
                    {
                        return dbK8sCrd;
                    }
                };

                database = k8sCrdProvider.get();
                initializer = new DbK8sCrdInitializer(reporter, database, cfg);
                break;
            default:
                throw new ImplementationError(String.format("Unrecognized database type '%s'", dbType));
        }
        return new Pair<>(initializer, database);
    }
}
