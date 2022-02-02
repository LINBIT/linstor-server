package com.linbit.linstor.propscon;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class PropsConSQLDbDriver implements PropsConDatabaseDriver
{
    private static final String TBL_PROP = DbConstants.TBL_PROPS_CONTAINERS;
    private static final String COL_INSTANCE = DbConstants.PROPS_INSTANCE;
    private static final String COL_KEY = DbConstants.PROP_KEY;
    private static final String COL_VALUE = DbConstants.PROP_VALUE;

    private static final String INSERT_ENTRY =
        " INSERT INTO " + TBL_PROP +
        " ( " + COL_INSTANCE + ", " + COL_KEY + ", " + COL_VALUE + " ) " +
        " VALUES ( ?, ?, ? )";
    private static final String UPDATE_ENTRY =
        " UPDATE " + TBL_PROP +
        " SET " + COL_VALUE + " = ? " +
        " WHERE " + COL_INSTANCE + " = ? AND " +
                    COL_KEY + " = ?";

    private static final String SELECT_ALL_ENTRIES_BY_INSTANCE =
        " SELECT " + COL_KEY + ", " + COL_VALUE + "\n" +
        " FROM " + TBL_PROP + "\n" +
        " WHERE " + COL_INSTANCE + " = ?";

    private static final String REMOVE_ENTRY =
        " DELETE FROM " + TBL_PROP + "\n" +
        "    WHERE " + COL_INSTANCE + " = ? \n" +
        "        AND " + COL_KEY + " = ?";

    private static final String REMOVE_ALL_ENTRIES =
        " DELETE FROM " + TBL_PROP + "\n" +
        "    WHERE " + COL_INSTANCE + " = ? ";

    private final ErrorReporter errorReporter;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    @Inject
    public PropsConSQLDbDriver(
        ErrorReporter errorReporterRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    public void persist(String instanceName, String key, String value, boolean isNew) throws DatabaseException
    {
        errorReporter.logTrace("Storing property %s", getId(instanceName, key, value));
        try
        {
            if (isNew)
            {
                insert(instanceName, key, value);
            }
            else
            {
                update(instanceName, key, value);
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("Property stored %s", getId(instanceName, key, value));
    }

    private void insert(String instanceName, String key, String value) throws SQLException
    {
        try (
            PreparedStatement stmt = getConnection().prepareStatement(INSERT_ENTRY);
        )
        {
            stmt.setString(1, instanceName.toUpperCase());
            stmt.setString(2, key);
            stmt.setString(3, value);

            stmt.executeUpdate();
        }
    }

    private void update(String instanceName, String key, String value) throws SQLException
    {
        try (
            PreparedStatement stmt = getConnection().prepareStatement(UPDATE_ENTRY);
        )
        {
            stmt.setString(1, value);
            stmt.setString(2, instanceName.toUpperCase());
            stmt.setString(3, key);

            stmt.executeUpdate();
        }
    }

    @Override
    public void remove(String instanceName, String key) throws DatabaseException
    {
        errorReporter.logTrace("Removing property %s", getId(instanceName, key));

        try (PreparedStatement stmt = getConnection().prepareStatement(REMOVE_ENTRY))
        {
            stmt.setString(1, instanceName.toUpperCase());
            stmt.setString(2, key);

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }

        errorReporter.logTrace("Property removed %s", getId(instanceName, key));
    }

    @Override
    public void remove(String instanceName, Set<String> keys) throws DatabaseException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(REMOVE_ENTRY))
        {
            stmt.setString(1, instanceName.toUpperCase());
            for (String key : keys)
            {
                errorReporter.logTrace("Removing property %s", getId(instanceName, key));

                stmt.setString(2, key);
                stmt.executeUpdate();

                errorReporter.logTrace("Property removed %s", getId(instanceName, key));
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    @Override
    public void removeAll(String instanceName) throws DatabaseException
    {
        errorReporter.logTrace("Removing all properties by instance %s", getId(instanceName));

        int rowsUpdated;
        try (
            PreparedStatement stmt = getConnection()
                .prepareStatement(REMOVE_ALL_ENTRIES)
        )
        {
            stmt.setString(1, instanceName.toUpperCase());
            rowsUpdated = stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace(
            "Removed all (%d) properties by instance %s",
            rowsUpdated,
            getId(instanceName)
        );
    }

    @Override
    public Map<String, String> loadAll(String instanceName) throws DatabaseException
    {
        errorReporter.logTrace("Loading properties for instance %s", getId(instanceName));
        Map<String, String> ret = new TreeMap<>();
        Connection connection = getConnection();
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_ALL_ENTRIES_BY_INSTANCE))
        {
            stmt.setString(1, instanceName.toUpperCase());

            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    String key = resultSet.getString(1);
                    String value = resultSet.getString(2);

                    ret.put(key, value);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace(
            "Loaded all (%d) properties for instance %s",
            ret.size(),
            getId(instanceName)
        );
        return ret;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }
}
