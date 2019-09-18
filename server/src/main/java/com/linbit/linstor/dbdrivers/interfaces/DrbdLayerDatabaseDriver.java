package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPool.InitMaps;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.utils.Pair;

import java.util.Map;
import java.util.Set;

public interface DrbdLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    // DrbdRscData methods
    void create(DrbdRscData drbdRscData) throws DatabaseException;
    void delete(DrbdRscData drbdRscData) throws DatabaseException;
    StateFlagsPersistence<DrbdRscData> getRscStateFlagPersistence();

    // DrbdRscDfnData methods
    void persist(DrbdRscDfnData drbdRscDfnData) throws DatabaseException;
    void delete(DrbdRscDfnData drbdRscDfnData) throws DatabaseException;
    SingleColumnDatabaseDriver<DrbdRscDfnData, TcpPortNumber> getTcpPortDriver();
    SingleColumnDatabaseDriver<DrbdRscDfnData, TransportType> getTransportTypeDriver();
    SingleColumnDatabaseDriver<DrbdRscDfnData, String> getRscDfnSecretDriver();
    SingleColumnDatabaseDriver<DrbdRscDfnData, Short> getPeerSlotsDriver();

    // DrbdVlmData methods
    void persist(DrbdVlmData drbdVlmData) throws DatabaseException;
    void delete(DrbdVlmData drbdVlmData) throws DatabaseException;
    SingleColumnDatabaseDriver<DrbdVlmData, StorPool> getExtStorPoolDriver();

    // DrbdVlmDfnData
    void persist(DrbdVlmDfnData drbdVlmDfnData) throws DatabaseException;
    void delete(DrbdVlmDfnData drbdVlmDfnData) throws DatabaseException;

    // methods only used for loading
    void loadLayerData(Map<ResourceName, ResourceDefinition> tmpRscDfnMapRef)
        throws DatabaseException;
    void clearLoadCache();
    Pair<? extends RscLayerObject, Set<RscLayerObject>> load(
        Resource rscRef,
        int idRef,
        String rscSuffixRef,
        RscLayerObject parentRef,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, InitMaps>> tmpStorPoolMapRef
    )
        throws DatabaseException;

}
