package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdVlmDfnObject;

public interface DrbdVlmDfnDatabaseDriver
{
    SingleColumnDatabaseDriver<DrbdVlmDfnObject, MinorNumber> getMinorDriver();

    SingleColumnDatabaseDriver<DrbdVlmDfnObject, Integer> getPeerSlotsDriver();

}
