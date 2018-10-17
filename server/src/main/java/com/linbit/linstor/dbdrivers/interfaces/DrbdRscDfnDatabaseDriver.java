package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.storage2.layer.data.DrbdRscDfnData;

public interface DrbdRscDfnDatabaseDriver
{
    SingleColumnDatabaseDriver<DrbdRscDfnData, TcpPortNumber> getPortDriver();

    SingleColumnDatabaseDriver<DrbdRscDfnData, TransportType> getTransportTypeDriver();

    SingleColumnDatabaseDriver<DrbdRscDfnData, String> getSecretDriver();

}
