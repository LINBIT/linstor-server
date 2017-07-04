package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.StorPoolData;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.utils.UuidUtils;

public class StorPoolDataDerbyDriver implements StorPoolDataDatabaseDriver
{
    private static final String TBL_NSP = DerbyConstants.TBL_NODE_STOR_POOL;

    private static final String NSP_UUID = DerbyConstants.UUID;
    private static final String NSP_NODE = DerbyConstants.NODE_NAME;
    private static final String NSP_POOL = DerbyConstants.POOL_NAME;
    private static final String NSP_DRIVER = DerbyConstants.DRIVER_NAME;

    private static final String NSP_SELECT =
        " SElECT " + NSP_UUID + ", " + NSP_NODE + ", " + NSP_POOL + ", " + NSP_DRIVER +
        " FROM " + TBL_NSP +
        " WHERE " + NSP_NODE + " = ? AND " +
        "       " + NSP_POOL + " = ?";
    private static final String NSP_INSERT =
        " INSERT INTO " + TBL_NSP +
        " VALUES (?, ?, ?, ?)";
    private static final String NSP_DELETE =
        " DELETE FROM " + TBL_NSP +
        " WHERE " + NSP_NODE + " = ? AND " +
        "       " + NSP_POOL + " = ?";


    private final Node node;
    private final StorPoolDefinition storPoolDfn;

    public StorPoolDataDerbyDriver(Node nodeRef, StorPoolDefinition storPoolDfnRef)
    {
        node = nodeRef;
        storPoolDfn = storPoolDfnRef;
    }

    @Override
    public void create(Connection con, StorPoolData storPoolData) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NSP_INSERT);
        stmt.setBytes(1, UuidUtils.asByteArray(storPoolData.getUuid()));
        stmt.setString(2, node.getName().value);
        stmt.setString(3, storPoolData.getName().value);
        stmt.setString(4, storPoolData.getDriverName());
        stmt.executeUpdate();
        stmt.close();
    }

    @Override
    public StorPoolData load(
        Connection con,
        AccessContext accCtx,
        TransactionMgr transMgr,
        SerialGenerator serGen
    )
        throws SQLException, AccessDeniedException
    {
        PreparedStatement stmt = con.prepareStatement(NSP_SELECT);
        stmt.setString(1, node.getName().value);
        stmt.setString(2, storPoolDfn.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        StorPoolData sp = null;
        if (resultSet.next())
        {
            sp = new StorPoolData(
                accCtx,
                storPoolDfn,
                transMgr,
                null,   // storageDriver, has to be null in the controller
                resultSet.getString(NSP_DRIVER),
                serGen,
                node,
                UuidUtils.asUUID(resultSet.getBytes(NSP_UUID))
            );
        }
        resultSet.close();
        stmt.close();

        return sp;
    }

    @Override
    public void delete(Connection con, StorPoolName storPoolName) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NSP_DELETE);

        stmt.setString(1, node.getName().value);
        stmt.setString(2, storPoolName.value);

        stmt.executeUpdate();
        stmt.close();
    }

    @Override
    public void ensureEntryExists(Connection con, StorPoolData storPoolData) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NSP_SELECT);
        stmt.setString(1, node.getName().value);
        stmt.setString(2, storPoolDfn.getName().value);
        ResultSet resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            boolean equals = true;
            equals &= storPoolData.getUuid().equals(UuidUtils.asUUID(resultSet.getBytes(NSP_UUID)));
            equals &= storPoolData.getDriverName().equals(resultSet.getString(NSP_DRIVER));
            if (!equals)
            {
                throw new DrbdSqlRuntimeException("A temporary StorPoolData instance is not allowed to override a persisted instance.");
            }
        }
        else
        {
            create(con, storPoolData);
        }

        resultSet.close();
        stmt.close();
    }

    public static void loadStorPools(Connection con, NodeData node)
    {
        // TODO Auto-generated method stub

    }
}
