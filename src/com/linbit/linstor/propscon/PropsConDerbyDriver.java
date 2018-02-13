package com.linbit.linstor.propscon;

import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class PropsConDerbyDriver implements PropsConDatabaseDriver
{
    private static final String TBL_PROP = DerbyConstants.TBL_PROPS_CONTAINERS;
    private static final String COL_INSTANCE = DerbyConstants.PROPS_INSTANCE;
    private static final String COL_KEY = DerbyConstants.PROP_KEY;
    private static final String COL_VALUE = DerbyConstants.PROP_VALUE;

    private static final String SELECT_ENTRY_FOR_UPDATE =
        " SELECT " + COL_INSTANCE + ", " + COL_KEY + ", " + COL_VALUE + "\n" +
        " FROM " + TBL_PROP + "\n" +
        " WHERE " + COL_INSTANCE + " = ? AND \n" +
        "       " + COL_KEY +      " = ? \n" +
        " FOR UPDATE OF " + COL_INSTANCE + "," + COL_KEY + ", " + COL_VALUE;

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

    @Inject
    public PropsConDerbyDriver(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
    }

    @Override
    public void persist(String instanceName, String key, String value, TransactionMgr transMgr) throws SQLException
    {
        persistImpl(instanceName, key, value, transMgr);
    }

    @Override
    public void persist(String instanceName, Map<String, String> props, TransactionMgr transMgr) throws SQLException
    {
        for (Entry<String, String> entry : props.entrySet())
        {
            persistImpl(instanceName, entry.getKey(), entry.getValue(), transMgr);
        }
    }

    private void persistImpl(String instanceName, String key, String value, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Storing property %s", getId(instanceName, key, value));
        try (
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(
                SELECT_ENTRY_FOR_UPDATE,
                ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE
            )
        )
        {
            String instanceUpper = instanceName.toUpperCase();
            stmt.setString(1, instanceUpper);
            stmt.setString(2, key);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    resultSet.updateString(3, value);
                    resultSet.updateRow();
                }
                else
                {
                    resultSet.moveToInsertRow();
                    resultSet.updateString(1, instanceUpper);
                    resultSet.updateString(2, key);
                    resultSet.updateString(3, value);
                    resultSet.insertRow();
                }
            }
        }
        errorReporter.logTrace("Property stored %s", getId(instanceName, key, value));
    }

    @Override
    public void remove(String instanceName, String key, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Removing property %s", getId(instanceName, key));

        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(REMOVE_ENTRY))
        {
            stmt.setString(1, instanceName.toUpperCase());
            stmt.setString(2, key);

            stmt.executeUpdate();
        }

        errorReporter.logTrace("Property removed %s", getId(instanceName, key));
    }

    @Override
    public void remove(String instanceName, Set<String> keys, TransactionMgr transMgr) throws SQLException
    {
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(REMOVE_ENTRY))
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
    }

    @Override
    public void removeAll(String instanceName, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Removing all properties by instance %s", getId(instanceName));

        int rowsUpdated;
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(REMOVE_ALL_ENTRIES))
        {
            stmt.setString(1, instanceName.toUpperCase());
            rowsUpdated = stmt.executeUpdate();
        }
        errorReporter.logTrace(
            "Removed all (%d) properties by instance %s",
            rowsUpdated,
            getId(instanceName)
        );
    }

    @Override
    public Map<String, String> load(String instanceName, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Loading properties for instance %s", getId(instanceName));
        Map<String, String> ret = new TreeMap<>();
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_ENTRIES_BY_INSTANCE))
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
        errorReporter.logTrace(
            "Loaded all (%d) properties for instance %s",
            ret.size(),
            getId(instanceName)
        );
        return ret;
    }

    private String getId(String instanceName)
    {
        return "(InstanceName=" + instanceName + ")";
    }

    private String getId(String instanceName, String key)
    {
        return "(InstanceName=" + instanceName + " Key=" + key + ")";
    }

    private String getId(String instanceName, String key, String value)
    {
        return "(InstanceName=" + instanceName + " Key=" + key + " Value=" + value + ")";
    }
}
