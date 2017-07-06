package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import com.linbit.MapDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.ConnectionDefinition;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface ResourceDefinitionDataDatabaseDriver
{
    public MapDatabaseDriver<NodeName, Map<Integer, ConnectionDefinition>> getConnectionMapDriver();

    public MapDatabaseDriver<VolumeNumber, VolumeDefinition> getVolumeMapDriver();

    public MapDatabaseDriver<NodeName, Resource> getResourceMapDriver();

    public StateFlagsPersistence getStateFlagsPersistence();

    public void create(Connection dbCon, ResourceDefinitionData resDfn)
        throws SQLException;

    public boolean exists(Connection dbCon)
        throws SQLException;

    public ResourceDefinitionData load(Connection dbCon, SerialGenerator serialGen, TransactionMgr transMgr)
        throws SQLException;

    void delete(Connection con)
        throws SQLException;
}
