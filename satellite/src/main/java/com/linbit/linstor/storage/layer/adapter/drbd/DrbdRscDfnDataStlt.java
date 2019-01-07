package com.linbit.linstor.storage.layer.adapter.drbd;

import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.storage.layer.data.DrbdRscDfnData;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;

public class DrbdRscDfnDataStlt extends BaseTransactionObject implements DrbdRscDfnData
{
    // data from controller - read only
    final TcpPortNumber port;
    final TransportType transportType;
    final String secret;

    // temporary data - satellite only

    public DrbdRscDfnDataStlt(
        TcpPortNumber portRef,
        TransportType transportTypeRef,
        String secretRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        port = portRef;
        transportType = transportTypeRef;
        secret = secretRef;

        transObjs = Arrays.asList();
    }

    @Override
    public TcpPortNumber getPort()
    {
        return port;
    }

    @Override
    public TransportType getTransportType()
    {
        return transportType;
    }

    @Override
    public String getSecret()
    {
        return secret;
    }
}
