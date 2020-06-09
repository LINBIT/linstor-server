package com.linbit.linstor.core;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.cfg.CtrlTomlConfig;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

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
    private static CommandLine commandLine;

    private static final String DB_USER = "linstor";
    private static final String DB_PASSWORD = "linstor";

    private static List<String> supportedDbs = Arrays.asList("h2", "postgresql");

    private static final String DB_CFG = "[db]\n" +
        "  user = \"%s\"\n" +
        "  password = \"%s\"\n" +
        "  connection_url = \"%s\"\n";

    @CommandLine.Command(name = "linstor-config", subcommands = {
        CmdSetPlainPort.class,
        CmdSetPlainListen.class,
        CmdCreateDBTomlConfig.class,
        CmdSqlScript.class,
        CmdMigrateDatabaseConfig.class
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
        private String dbpath;

        @Override
        public Object call() throws Exception
        {
            OutputStream os = System.out;

            DatabaseDriverInfo dbInfo = DatabaseDriverInfo.createDriverInfo(dbtype);

            if (dbInfo != null)
            {
                os.write("# Basic linstor configuration toml file\n# For more options check documentation\n\n"
                    .getBytes(StandardCharsets.UTF_8));
                os.write(
                    String.format(
                        DB_CFG,
                        DB_USER,
                        DB_PASSWORD,
                        dbInfo.jdbcUrl(dbpath)
                    ).getBytes(StandardCharsets.UTF_8)
                );
            }
            else
            {
                System.err.println(
                    String.format(
                        "Database type '%s' not supported. Use one of: '%s'",
                        dbtype,
                        String.join("', '", supportedDbs))
                );
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
        private File sqlFile = null;

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

        GenericObjectPoolConfig<PoolableConnection> poolConfig = new GenericObjectPoolConfig<PoolableConnection>();
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
}
