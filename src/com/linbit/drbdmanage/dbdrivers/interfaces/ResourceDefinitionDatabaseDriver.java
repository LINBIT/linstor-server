package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.util.Map;

import com.linbit.MapDatabaseDriver;
import com.linbit.drbdmanage.ConnectionDefinition;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface ResourceDefinitionDatabaseDriver
{
    void setConnection(Connection dbCon);

    MapDatabaseDriver<NodeName, Map<Integer, ConnectionDefinition>> getConnectionMapDriver();

    MapDatabaseDriver<VolumeNumber, VolumeDefinition> getVolumeMapDriver();

    MapDatabaseDriver<NodeName, Resource> getResourceMapDriver();

    StateFlagsPersistence getStateFlagsPersistence();
}
