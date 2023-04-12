package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;

public interface DrbdLayerDatabaseDriver
{
    LayerResourceIdDatabaseDriver getIdDriver();

    // DrbdRscData methods
    void create(DrbdRscData<?> drbdRscData) throws DatabaseException;
    void delete(DrbdRscData<?> drbdRscData) throws DatabaseException;
    StateFlagsPersistence<DrbdRscData<?>> getRscStateFlagPersistence();
    SingleColumnDatabaseDriver<DrbdRscData<?>, NodeId> getNodeIdDriver();

    // DrbdRscDfnData methods
    void persist(DrbdRscDfnData<?> drbdRscDfnData) throws DatabaseException;
    void delete(DrbdRscDfnData<?> drbdRscDfnData) throws DatabaseException;
    SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber> getTcpPortDriver();
    SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType> getTransportTypeDriver();
    SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String> getRscDfnSecretDriver();
    SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short> getPeerSlotsDriver();

    // DrbdVlmData methods
    void persist(DrbdVlmData<?> drbdVlmData) throws DatabaseException;
    void delete(DrbdVlmData<?> drbdVlmData) throws DatabaseException;
    SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> getExtStorPoolDriver();

    // DrbdVlmDfnData
    void persist(DrbdVlmDfnData<?> drbdVlmDfnData) throws DatabaseException;
    void delete(DrbdVlmDfnData<?> drbdVlmDfnData) throws DatabaseException;
}
