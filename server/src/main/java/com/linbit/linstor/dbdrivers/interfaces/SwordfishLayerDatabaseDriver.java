package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.data.provider.swordfish.SfInitiatorData;
import com.linbit.linstor.storage.data.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;

public interface SwordfishLayerDatabaseDriver
{

    SingleColumnDatabaseDriver<SfVlmDfnData, String> getVlmDfnOdataDriver();

    void persist(SfVlmDfnData vlmDfnDataRef) throws DatabaseException;
    void delete(SfVlmDfnData vlmDfnDataRef) throws DatabaseException;

    void persist(SfInitiatorData sfInitiatorDataRef) throws DatabaseException;
    void delete(SfInitiatorData sfInitiatorDataRef) throws DatabaseException;

    void persist(SfTargetData sfTargetDataRef) throws DatabaseException;
    void delete(SfTargetData sfTargetDataRef) throws DatabaseException;
}
