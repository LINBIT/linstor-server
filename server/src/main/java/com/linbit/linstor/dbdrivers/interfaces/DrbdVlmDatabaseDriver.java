package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdVlmObject;

public interface DrbdVlmDatabaseDriver
{
    SingleColumnDatabaseDriver<DrbdVlmObject, String> getMetaDiskDriver();

}
