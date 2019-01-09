package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject;

public interface DrbdRscDfnDatabaseDriver
{
    SingleColumnDatabaseDriver<DrbdRscDfnObject, TcpPortNumber> getPortDriver();

    SingleColumnDatabaseDriver<DrbdRscDfnObject, TransportType> getTransportTypeDriver();

    SingleColumnDatabaseDriver<DrbdRscDfnObject, String> getSecretDriver();

}
