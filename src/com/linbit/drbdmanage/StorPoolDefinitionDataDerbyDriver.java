package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.utils.UuidUtils;

public class StorPoolDefinitionDataDerbyDriver implements StorPoolDefinitionDataDatabaseDriver
{
    private static final String TBL_SPD = DerbyConstants.TBL_STOR_POOL_DEFINITIONS;

    private static final String SPD_UUID = DerbyConstants.UUID;
    private static final String SPD_NAME = DerbyConstants.POOL_NAME;
    private static final String SPD_DSP_NAME = DerbyConstants.POOL_DSP_NAME;

    private static final String SPD_INSERT =
        " INSERT INTO " + TBL_SPD +
        " VALUES (?, ?, ?)";
    private static final String SPD_SELECT =
        " SELECT " + SPD_UUID + ", " + SPD_NAME + ", " + SPD_DSP_NAME +
        " FROM " + TBL_SPD +
        " WHERE " + SPD_NAME + " = ?";
    private static final String SPD_DELETE =
        " DELETE FROM " + TBL_SPD +
        " WHERE " + SPD_NAME + " = ?";

    private final StorPoolName name;

    public StorPoolDefinitionDataDerbyDriver(StorPoolName name)
    {
        this.name = name;
    }

    @Override
    public void create(Connection con, StorPoolDefinitionData spdd) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(SPD_INSERT);
        stmt.setBytes(1, UuidUtils.asByteArray(spdd.getUuid()));
        stmt.setString(2, spdd.getName().value);
        stmt.setString(3, spdd.getName().displayValue);
        stmt.executeUpdate();
        stmt.close();
    }

    @Override
    public StorPoolDefinitionData load(Connection con, AccessContext accCtx, TransactionMgr transMgr) throws SQLException, AccessDeniedException
    {
        PreparedStatement stmt = con.prepareStatement(SPD_SELECT);
        stmt.setString(1, name.value);
        ResultSet resultSet = stmt.executeQuery();

        StorPoolDefinitionData spdd = null;
        if (resultSet.next())
        {
            UUID id = UuidUtils.asUUID(resultSet.getBytes(SPD_UUID));
            spdd = new StorPoolDefinitionData(accCtx, name, transMgr, id);
        }
        resultSet.close();
        stmt.close();
        return spdd;
    }

    @Override
    public void delete(Connection con) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(SPD_DELETE);
        stmt.setString(1, name.value);
        stmt.executeUpdate();
        stmt.close();
    }

}
