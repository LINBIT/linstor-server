package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.storage2.layer.data.DrbdVlmDfnData;

public interface DrbdVlmDfnDatabaseDriver
{
    SingleColumnDatabaseDriver<DrbdVlmDfnData, MinorNumber> getMinorDriver();

    SingleColumnDatabaseDriver<DrbdVlmDfnData, Integer> getPeerSlotsDriver();

}
