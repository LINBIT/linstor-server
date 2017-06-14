package com.linbit.drbdmanage.security;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.linbit.InvalidNameException;
import com.linbit.drbdmanage.DatabaseSetter;
import com.linbit.drbdmanage.dbcp.DbConnectionPool;
import com.linbit.drbdmanage.dbdrivers.DerbyDriver;
import com.linbit.drbdmanage.logging.StdErrorReporter;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.Identity;
import com.linbit.drbdmanage.security.Privilege;
import com.linbit.drbdmanage.security.PrivilegeSet;
import com.linbit.drbdmanage.security.Role;
import com.linbit.drbdmanage.security.SecurityType;

public abstract class DerbyBase implements DerbyConstants
{

    private static final String DB_URL = "jdbc:derby:directory:database";
    private static final String DB_USER = "drbdmanage";
    private static final String DB_PASSWORD = "linbit";
    private static final Properties DB_PROPS = new Properties();

    private List<Statement> statements = new ArrayList<>();
    private static Connection con;
    private static DbConnectionPool dbConnPool;
    private static List<Connection> connections = new ArrayList<>();

    protected static final AccessContext sysCtx;
    private static boolean initialized = false;
    private static DbDerbyPersistence secureDbDriver;

    private static String[] createTables;
    private static String[] defaultValues;
    private static String[] truncateTables;
    private static String[] dropTables;

    static
    {
        PrivilegeSet sysPrivs = new PrivilegeSet(Privilege.PRIV_SYS_ALL);

        sysCtx = new AccessContext(
            Identity.SYSTEM_ID,
            Role.SYSTEM_ROLE,
            SecurityType.SYSTEM_TYPE,
            sysPrivs
        );
        try
        {
            sysCtx.privEffective.enablePrivileges(Privilege.PRIV_SYS_ALL);
        }
        catch (AccessDeniedException iAmNotRootExc)
        {
            throw new RuntimeException(iAmNotRootExc);
        }
    }

    public DerbyBase(String[] createTables, String[] defaultValues, String[] truncateTables, String[] dropTables) throws SQLException
    {
        if (!initialized)
        {
            DerbyBase.createTables = createTables;
            DerbyBase.defaultValues = defaultValues;
            DerbyBase.truncateTables = truncateTables;
            DerbyBase.dropTables = dropTables;

            createTables();
            insertDefaults();

            try
            {
                Identity.load(dbConnPool, secureDbDriver);
                SecurityType.load(dbConnPool, secureDbDriver);
                Role.load(dbConnPool, secureDbDriver);
            }
            catch (SQLException | InvalidNameException exc)
            {
                throw new RuntimeException(exc);
            }


            initialized = true;
        }
    }

    @BeforeClass
    public static void setUpBeforeClass() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
    {
        // load the clientDriver...
        Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
        DB_PROPS.setProperty("create", "true");
        DB_PROPS.setProperty("user", DB_USER);
        DB_PROPS.setProperty("password", DB_PASSWORD);

        dbConnPool = new DbConnectionPool();
        dbConnPool.initializeDataSource(DB_URL, DB_PROPS);
        con = dbConnPool.getConnection();

        secureDbDriver = new DbDerbyPersistence(sysCtx);
        DerbyDriver persistenceDbDriver = new DerbyDriver(
            new StdErrorReporter("TESTING"),
            sysCtx
        );
        DatabaseSetter.setDatabaseClasses(
            secureDbDriver,
            persistenceDbDriver
        );
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        dropTables();

        con.close();
        dbConnPool.shutdown();
    }

    @Before
    public void setUp() throws SQLException
    {
        truncateTables();
        insertDefaults();
    }

    @After
    public void tearDown() throws SQLException
    {
        for (Statement statement : statements)
        {
            statement.close();
        }
        for (Connection connection : connections)
        {
            connection.close();
        }
        connections.clear();
    }

    protected static Connection getConnection() throws SQLException
    {
        Connection connection = dbConnPool.getConnection();
        connection.setAutoCommit(false);
        connections.add(connection);
        return connection;
    }

    protected void add(Statement stmt)
    {
        statements.add(stmt);
    }

    private void createTables() throws SQLException
    {
        for (int idx = 0; idx < createTables.length; ++idx)
        {
            createTable(con, true, idx);
        }
        con.commit();
    }

    private void insertDefaults() throws SQLException
    {
        for (String insert : defaultValues)
        {
            try (PreparedStatement stmt = con.prepareStatement(insert))
            {
                stmt.executeUpdate();
            }
        }
        con.commit();
    }

    private static void dropTables() throws SQLException
    {
        for (int idx = 0; idx < dropTables.length; ++idx)
        {
            dropTable(con, idx);
        }
    }

    private void truncateTables() throws SQLException
    {
        for (String sql : truncateTables)
        {
            PreparedStatement stmt = con.prepareStatement(sql);
            stmt.executeUpdate();
            stmt.close();
        }
    }

    private void createTable(Connection connection, boolean dropIfExists, int idx) throws SQLException
    {
        try
        {
            try (PreparedStatement stmt = connection.prepareStatement(createTables[idx]))
            {
//                System.out.print("creating... " + createTables[idx]);
                stmt.executeUpdate();
//                System.out.println("... done");
            }
        }
        catch (SQLException sqlExc)
        {
            String sqlState = sqlExc.getSQLState();
            if ("X0Y32".equals(sqlState)) // table already exists
            {
                if (dropIfExists)
                {
//                    System.out.print("exists, ");
                    dropTable(connection, dropTables.length - 1 - idx);
                    createTable(connection, false, idx);
                }
                else
                {
                    throw sqlExc;
                }
            }
            else
            {
                throw sqlExc;
            }
        }
        connection.commit();
    }

    private static void dropTable(Connection connection, int idx) throws SQLException
    {
        try (PreparedStatement stmt = connection.prepareStatement(dropTables[idx]))
        {
//            System.out.print("dropping... " + dropTables[idx]);
            stmt.executeUpdate();
//            System.out.println("... done");
        }
        catch (SQLException sqlExc)
        {
            if ("42Y55".equals(sqlExc.getSQLState()))
            {
                // table does not exists.... yay - ignore
            }
            else
            {
                throw sqlExc;
            }
        }
        connection.commit();
    }
}
