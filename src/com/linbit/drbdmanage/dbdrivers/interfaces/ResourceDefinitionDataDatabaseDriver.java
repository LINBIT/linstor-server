package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import com.linbit.MapDatabaseDriver;
import com.linbit.drbdmanage.ConnectionDefinition;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface ResourceDefinitionDataDatabaseDriver
{
    public MapDatabaseDriver<NodeName, Map<Integer, ConnectionDefinition>> getConnectionMapDriver();

    public MapDatabaseDriver<VolumeNumber, VolumeDefinition> getVolumeMapDriver();

    public MapDatabaseDriver<NodeName, Resource> getResourceMapDriver();

    public StateFlagsPersistence getStateFlagsPersistence();

    public void create(Connection dbCon) throws SQLException;

    public boolean exists(Connection dbCon) throws SQLException;
}
