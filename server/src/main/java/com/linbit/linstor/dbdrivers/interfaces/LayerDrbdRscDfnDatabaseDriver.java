package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;

public interface LayerDrbdRscDfnDatabaseDriver extends GenericDatabaseDriver<DrbdRscDfnData<?>>
{
    SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber> getTcpPortDriver();

    SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType> getTransportTypeDriver();

    SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String> getRscDfnSecretDriver();

    SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short> getPeerSlotsDriver();
}
