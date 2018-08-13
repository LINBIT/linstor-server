package com.linbit.linstor.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.GenericDbUtils;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import picocli.CommandLine;

public class LinstorConfig
{
    private static CommandLine commandLine;

    private static final String DB_USER = "linstor";
    private static final String DB_PASSWORD = "linstor";

    private static List<String> supportedDbs = Arrays.asList("h2", "derby", "postgresql");

    @CommandLine.Command(name = "linstor-config", subcommands = {
        CmdSetPlainPort.class,
        CmdSetPlainListen.class,
        CmdCreateDbXMLConfig.class
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
        description = "Write a database xml configuration file to standard out."
    )
    private static class CmdCreateDbXMLConfig implements Callable<Object>
    {
        @CommandLine.Option(names = {"--dbtype"}, description = "Specify the database type. ['h2', 'derby']")
        private String dbtype = "h2";

        @CommandLine.Parameters(description = "Path to the database")
        private String dbpath;

        @Override
        public Object call() throws Exception
        {
            OutputStream os = System.out;

            final String dbCfg = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n" +
                "<!DOCTYPE properties SYSTEM \"http://java.sun.com/dtd/properties.dtd\">\n" +
                "<properties>\n" +
                "  <comment>LinStor database configuration</comment>\n" +
                "  <entry key=\"user\">%s</entry>\n" +
                "  <entry key=\"password\">%s</entry>\n" +
                "  <entry key=\"connection-url\">%s</entry>\n" +
                "</properties>\n";

            DatabaseDriverInfo dbInfo = DatabaseDriverInfo.createDriverInfo(dbtype);

            if (dbInfo != null)
            {
                os.write(
                    String.format(
                        dbCfg,
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
            }
            return null;
        }
    }

    @CommandLine.Command(name = "set-plain-port", description = "Set the controller plain tcp port.")
    private static class CmdSetPlainPort implements Callable<Object>
    {
        @CommandLine.Parameters(index = "0", description = "Database configuration file.")
        private File dbCfgFile = new File("./database.cfg");

        @CommandLine.Parameters(index = "1", description = "New Port number.")
        private int controllerPort = ApiConsts.DFLT_CTRL_PORT_PLAIN;

        @Override
        public Object call() throws Exception
        {
            try (PoolingDataSource<PoolableConnection> dataSource =
                     initConnectionProviderFromCfg(dbCfgFile);
                 Connection con = dataSource.getConnection())
            {
                final String stmt = "UPDATE PROPS_CONTAINERS SET PROP_VALUE='%d' " +
                    "WHERE PROPS_INSTANCE='CTRLCFG' AND PROP_KEY='netcom/PlainConnector/port'";
                GenericDbUtils.executeStatement(con, String.format(stmt, controllerPort));
                con.commit();
                System.out.println("Controller plain port set to " + controllerPort);
            }
            catch (IOException ioExc)
            {
                System.err.println(String.format("Unable to parse `database.cfg` file '%s':", dbCfgFile.toString()));
                System.err.println(ioExc.getMessage());
            }
            return null;
        }
    }

    @CommandLine.Command(name = "set-plain-listen", description = "Set the controller plain listen/bind address.")
    private static class CmdSetPlainListen implements Callable<Object>
    {
        @CommandLine.Parameters(index = "0", description = "Database configuration file.")
        private File dbCfgFile = new File("./database.cfg");

        @CommandLine.Parameters(index = "1", description = "new Port number.")
        private String listenAddress = "::0";

        @Override
        public Object call() throws Exception
        {
            try (PoolingDataSource<PoolableConnection> dataSource =
                     initConnectionProviderFromCfg(dbCfgFile);
                 Connection con = dataSource.getConnection())
            {
                final String stmt = "UPDATE PROPS_CONTAINERS SET PROP_VALUE='%s' " +
                    "WHERE PROPS_INSTANCE='CTRLCFG' AND PROP_KEY='netcom/PlainConnector/bindaddress'";
                GenericDbUtils.executeStatement(con, String.format(stmt, listenAddress));
                con.commit();

                System.out.println("Controller plain listen address set to " + listenAddress);
            }
            catch (IOException ioExc)
            {
                System.err.println(String.format("Unable to parse `database.cfg` file '%s':", dbCfgFile.toString()));
                System.err.println(ioExc.getMessage());
            }

            return null;
        }
    }

    public static void main(String[] args)
    {
        commandLine = new CommandLine(new LinstorConfigCmd());

        commandLine.parseWithHandler(new CommandLine.RunLast(), System.err, args);
    }

    private static PoolingDataSource<PoolableConnection> initConnectionProvider(
        final String connUrl,
        final String user,
        final String password)
    {
        Properties dbProps = new Properties();
        dbProps.setProperty("user", user);
        dbProps.setProperty("password", password);
        ConnectionFactory connFactory = new DriverManagerConnectionFactory(
            connUrl,
            dbProps
        );
        PoolableConnectionFactory poolConnFactory = new PoolableConnectionFactory(connFactory, null);

        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setFairness(true);

        GenericObjectPool<PoolableConnection> connPool = new GenericObjectPool<>(poolConnFactory, poolConfig);

        poolConnFactory.setPool(connPool);

        return new PoolingDataSource<>(connPool);
    }

    private static PoolingDataSource<PoolableConnection> initConnectionProviderFromCfg(final File cfg)
        throws IOException
    {
        Properties dbProps = new Properties();
        try (FileInputStream fis = new FileInputStream(cfg))
        {
            dbProps.loadFromXML(fis);
        }

        return initConnectionProvider(
            dbProps.getProperty("connection-url"),
            dbProps.getProperty("user"),
            dbProps.getProperty("password")
        );
    }
}
