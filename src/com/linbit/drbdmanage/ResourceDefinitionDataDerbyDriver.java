package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.linbit.MapDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.ObjectProtectionDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;
import com.linbit.utils.UuidUtils;

public class ResourceDefinitionDataDerbyDriver implements ResourceDefinitionDataDatabaseDriver
{
    private static final String TBL_RES_DEF = DerbyConstants.TBL_RESOURCE_DEFINITIONS;

    private static final String RD_UUID = DerbyConstants.UUID;
    private static final String RD_RES_NAME = DerbyConstants.RESOURCE_NAME;
    private static final String RD_RES_DSP_NAME = DerbyConstants.RESOURCE_DSP_NAME;

    private static final String RD_SELECT =
        " SELECT " + RD_UUID + ", " + RD_RES_NAME + ", " + RD_RES_DSP_NAME +
        " FROM " + TBL_RES_DEF +
        " WHERE " + RD_RES_NAME + " = ?";
    private static final String RD_INSERT =
        " INSERT INTO " + TBL_RES_DEF +
        " VALUES (?, ?, ?)";

    private ResourceName resName;

    public ResourceDefinitionDataDerbyDriver(ResourceName resName)
    {
        this.resName = resName;
    }

    @Override
    public void create(Connection con, ResourceDefinitionData resDfn) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(RD_INSERT);
        stmt.setBytes(1, UuidUtils.asByteArray(resDfn.getUuid()));
        stmt.setString(2, resName.value);
        stmt.setString(3, resName.displayValue);
        stmt.executeUpdate();
        stmt.close();
    }

    @Override
    public boolean exists(Connection con) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(RD_SELECT);
        stmt.setString(1, resName.value);
        ResultSet resultSet = stmt.executeQuery();

        return resultSet.next();
    }

    @Override
    public ResourceDefinitionData load(Connection con, SerialGenerator serialGen, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(RD_SELECT);
        stmt.setString(1, resName.value);
        ResultSet resultSet = stmt.executeQuery();

        ResourceDefinitionData ret = null;
        if (resultSet.next())
        {
            ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                ObjectProtection.buildPath(resName)
            );
            ObjectProtection objProt = objProtDriver.loadObjectProtection(con);
            if (objProt != null)
            {
                ret = new ResourceDefinitionData(
                    objProt,
                    UuidUtils.asUUID(resultSet.getBytes(RD_UUID)),
                    resName,
                    serialGen,
                    transMgr
                );
            }
        }

        return ret;
    }

    @Override
    public MapDatabaseDriver<NodeName, Map<Integer, ConnectionDefinition>> getConnectionMapDriver()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MapDatabaseDriver<VolumeNumber, VolumeDefinition> getVolumeMapDriver()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MapDatabaseDriver<NodeName, Resource> getResourceMapDriver()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StateFlagsPersistence getStateFlagsPersistence()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
