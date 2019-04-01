package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;

import java.sql.SQLException;

public interface DrbdLayerDatabaseDriver
{
    ResourceLayerIdDatabaseDriver getIdDriver();

    // DrbdRscData methods
    void create(DrbdRscData drbdRscData) throws SQLException;
    void delete(DrbdRscData drbdRscData) throws SQLException;
    StateFlagsPersistence<DrbdRscData> getRscStateFlagPersistence();

    // DrbdRscDfnData methods
    void persist(DrbdRscDfnData drbdRscDfnData) throws SQLException;
    void delete(DrbdRscDfnData drbdRscDfnData) throws SQLException;
    SingleColumnDatabaseDriver<DrbdRscDfnData, TcpPortNumber> getTcpPortDriver();
    SingleColumnDatabaseDriver<DrbdRscDfnData, TransportType> getTransportTypeDriver();
    SingleColumnDatabaseDriver<DrbdRscDfnData, String> getRscDfnSecretDriver();
    SingleColumnDatabaseDriver<DrbdRscDfnData, Short> getPeerSlotsDriver();

    // DrbdVlmData methods
    void persist(DrbdVlmData drbdVlmData) throws SQLException;
    void delete(DrbdVlmData drbdVlmData) throws SQLException;

    // DrbdVlmDfnData
    void persist(DrbdVlmDfnData drbdVlmDfnData) throws SQLException;
    void delete(DrbdVlmDfnData drbdVlmDfnData) throws SQLException;

}
