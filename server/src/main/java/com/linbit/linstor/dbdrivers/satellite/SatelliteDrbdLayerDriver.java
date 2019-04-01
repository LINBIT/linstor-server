package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;

import javax.inject.Inject;

import java.sql.SQLException;

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
    public void create(DrbdRscData drbdRscDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(DrbdRscData drbdRscDataRef) throws SQLException
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
    public void persist(DrbdRscDfnData drbdRscDfnDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(DrbdRscDfnData drbdRscDfnDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public void persist(DrbdVlmData drbdVlmDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(DrbdVlmData drbdVlmDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public void persist(DrbdVlmDfnData drbdVlmDfnDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(DrbdVlmDfnData drbdVlmDfnDataRef) throws SQLException
    {
        // no-op
    }
}
