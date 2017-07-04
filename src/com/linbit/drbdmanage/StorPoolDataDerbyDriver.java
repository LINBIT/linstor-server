package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.StorPoolData;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.ObjectProtectionDatabaseDriver;
import com.linbit.utils.UuidUtils;

public class StorPoolDataDerbyDriver implements StorPoolDataDatabaseDriver
{
    private static final String TBL_NSP = DerbyConstants.TBL_NODE_STOR_POOL;

    private static final String NSP_UUID = DerbyConstants.UUID;
    private static final String NSP_NODE = DerbyConstants.NODE_NAME;
    private static final String NSP_POOL = DerbyConstants.POOL_NAME;
    private static final String NSP_DRIVER = DerbyConstants.DRIVER_NAME;

    private static final String NSP_SELECT_BY_NODE =
        " SElECT " + NSP_UUID + ", " + NSP_NODE + ", " + NSP_POOL + ", " + NSP_DRIVER +
        " FROM " + TBL_NSP +
        " WHERE " + NSP_NODE + " = ?";
    private static final String NSP_SELECT = NSP_SELECT_BY_NODE +
        " AND "  + NSP_POOL + " = ?";
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
        TransactionMgr transMgr,
        SerialGenerator serGen
    )
        throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NSP_SELECT);
        stmt.setString(1, node.getName().value);
        stmt.setString(2, storPoolDfn.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        StorPoolData sp = null;
        if (resultSet.next())
        {
            ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                ObjectProtection.buildPathSP(storPoolDfn.getName())
            );
            ObjectProtection objProt = objProtDriver.loadObjectProtection(con);

            sp = new StorPoolData(
                UuidUtils.asUUID(resultSet.getBytes(NSP_UUID)),
                objProt,
                storPoolDfn,
                transMgr,
                null,   // storageDriver, has to be null in the controller
                resultSet.getString(NSP_DRIVER),
                serGen,
                node
            );
        }
        resultSet.close();
        stmt.close();

        return sp;
    }

    public static List<StorPoolData> loadStorPools(Connection con, NodeData node, TransactionMgr transMgr, SerialGenerator serGen) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NSP_SELECT_BY_NODE);
        stmt.setString(1, node.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        List<StorPoolData> storPoolList = new ArrayList<>();
        try
        {
            while(resultSet.next())
            {
                StorPoolName storPoolName;
                    storPoolName = new StorPoolName(resultSet.getString(NSP_POOL));

                ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                    ObjectProtection.buildPathSP(storPoolName)
                    );
                ObjectProtection objProt = objProtDriver.loadObjectProtection(con);

                StorPoolDefinitionDataDatabaseDriver storPoolDefDriver = DrbdManage.getStorPoolDefinitionDataDriver(storPoolName);
                StorPoolDefinitionData storPoolDef = storPoolDefDriver.load(con);


                StorPoolData storPoolData = new StorPoolData(
                    UuidUtils.asUUID(resultSet.getBytes(NSP_UUID)),
                    objProt,
                    storPoolDef,
                    transMgr,
                    null, // controller should not have an instance of storage driver.
                    resultSet.getString(NSP_DRIVER),
                    serGen,
                    node
                );

                storPoolList.add(storPoolData);
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new DrbdSqlRuntimeException(
                String.format("Invalid storage name loaded from Table %s: %s ", TBL_NSP, resultSet.getString(NSP_POOL)),
                invalidNameExc
            );
        }


        return storPoolList;
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

}
