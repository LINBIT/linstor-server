package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.provider.swordfish.SfInitiatorData;
import com.linbit.linstor.storage.data.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;

import java.sql.SQLException;

public interface SwordfishLayerDatabaseDriver
{

    SingleColumnDatabaseDriver<SfVlmDfnData, String> getVlmDfnOdataDriver();

    void persist(SfVlmDfnData vlmDfnDataRef) throws SQLException;
    void delete(SfVlmDfnData vlmDfnDataRef) throws SQLException;

    void persist(SfInitiatorData sfInitiatorDataRef) throws SQLException;
    void delete(SfInitiatorData sfInitiatorDataRef) throws SQLException;

    void persist(SfTargetData sfTargetDataRef) throws SQLException;
    void delete(SfTargetData sfTargetDataRef) throws SQLException;
}
