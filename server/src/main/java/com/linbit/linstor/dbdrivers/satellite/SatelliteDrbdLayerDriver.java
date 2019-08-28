package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.ImplementationError;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinition.TransportType;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPool.InitMaps;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.utils.Pair;

import javax.inject.Inject;

import java.util.Map;
import java.util.Set;

public class SatelliteDrbdLayerDriver implements DrbdLayerDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final StateFlagsPersistence<?> noopStateFlagsDriver = new SatelliteFlagDriver();
    private final ResourceLayerIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteResourceLayerIdDriver();

    @Inject
    public SatelliteDrbdLayerDriver()
    {
    }

    @Override
    public void create(DrbdRscData drbdRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(DrbdRscData drbdRscDataRef) throws DatabaseException
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<DrbdRscData> getRscStateFlagPersistence()
    {
        return (StateFlagsPersistence<DrbdRscData>) noopStateFlagsDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdVlmData, StorPool> getExtStorPoolDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdVlmData, StorPool>) noopSingleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData, TcpPortNumber> getTcpPortDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnData, TcpPortNumber>) noopSingleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData, TransportType> getTransportTypeDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnData, TransportType>) noopSingleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData, String> getRscDfnSecretDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnData, String>) noopSingleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData, Short> getPeerSlotsDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnData, Short>) noopSingleColDriver;
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public void persist(DrbdRscDfnData drbdRscDfnDataRef)
    {
        // no-op
    }

    @Override
    public void delete(DrbdRscDfnData drbdRscDfnDataRef)
    {
        // no-op
    }

    @Override
    public void persist(DrbdVlmData drbdVlmDataRef)
    {
        // no-op
    }

    @Override
    public void delete(DrbdVlmData drbdVlmDataRef)
    {
        // no-op
    }

    @Override
    public void persist(DrbdVlmDfnData drbdVlmDfnDataRef)
    {
        // no-op
    }

    @Override
    public void delete(DrbdVlmDfnData drbdVlmDfnDataRef)
    {
        // no-op
    }

    @Override
    public void clearLoadCache()
    {
        throw new ImplementationError("This method should never be called on satellite");
    }

    @Override
    public Pair<? extends RscLayerObject, Set<RscLayerObject>> load(
        Resource rscRef,
        int idRef,
        String rscSuffixRef,
        RscLayerObject parentRef,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, InitMaps>> tmpStorPoolMapRef
    )
        throws DatabaseException
    {
        throw new ImplementationError("This method should never be called on satellite");
    }

    @Override
    public void loadLayerData(Map<ResourceName, ResourceDefinition> tmpRscDfnMapRef) throws DatabaseException
    {
        throw new ImplementationError("This method should never be called on satellite");
    }
}

