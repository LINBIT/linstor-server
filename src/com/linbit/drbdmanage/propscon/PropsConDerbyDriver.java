package com.linbit.drbdmanage.propscon;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;

import java.util.Set;
import java.util.TreeMap;

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
        " FOR UPDATE OF " + COL_INSTANCE + "," + COL_KEY + ", "+ COL_VALUE;

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

    public PropsConDerbyDriver(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
    }

    @Override
    public void persist(String instanceName, String key, String value, TransactionMgr transMgr) throws SQLException
    {
        if (transMgr != null)
        {
            persistImpl(instanceName, key, value, transMgr);
        }
    }

    @Override
    public void persist(String instanceName, Map<String, String> props, TransactionMgr transMgr) throws SQLException
    {
        if (transMgr!= null)
        {
            for (Entry<String, String> entry : props.entrySet())
            {
                persistImpl(instanceName, entry.getKey(), entry.getValue(), transMgr);
            }
        }
    }

    private void persistImpl(String instanceName, String key, String value, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(
            SELECT_ENTRY_FOR_UPDATE,
            ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_UPDATABLE
        );

        String instanceUpper = instanceName.toUpperCase();
        stmt.setString(1, instanceUpper);
        stmt.setString(2, key);

        ResultSet resultSet = stmt.executeQuery();
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

        resultSet.close();
        stmt.close();
    }

    @Override
    public void remove(String instanceName, String key, TransactionMgr transMgr) throws SQLException
    {
        if (transMgr != null)
        {
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(REMOVE_ENTRY);

            stmt.setString(1, instanceName.toUpperCase());
            stmt.setString(2, key);

            stmt.executeUpdate();
            stmt.close();
        }
    }

    @Override
    public void remove(String instanceName, Set<String> keys, TransactionMgr transMgr) throws SQLException
    {
        if (transMgr != null)
        {
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(REMOVE_ENTRY);

            stmt.setString(1, instanceName.toUpperCase());

            for (String key : keys)
            {
                stmt.setString(2, key);
                stmt.executeUpdate();
            }
            stmt.close();
        }
    }

    @Override
    public void removeAll(String instanceName, TransactionMgr transMgr) throws SQLException
    {
        if (transMgr != null)
        {
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(REMOVE_ALL_ENTRIES);

            stmt.setString(1, instanceName.toUpperCase());
            stmt.executeUpdate();

            stmt.close();
        }
    }

    @Override
    public Map<String, String> load(String instanceName, TransactionMgr transMgr) throws SQLException
    {
        Map<String, String> ret = new TreeMap<>();
        if (transMgr != null)
        {
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_ENTRIES_BY_INSTANCE);

            stmt.setString(1, instanceName.toUpperCase());

            ResultSet resultSet = stmt.executeQuery();

            while (resultSet.next())
            {
                String key = resultSet.getString(1);
                String value = resultSet.getString(2);

                ret.put(key, value);
            }
            resultSet.close();
            stmt.close();
        }
        return ret;
    }
}
