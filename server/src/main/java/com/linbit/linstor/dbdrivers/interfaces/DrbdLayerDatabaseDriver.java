package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.ResourceDefinition.TransportType;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;

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


}
