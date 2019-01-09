package com.linbit.linstor.storage.layer.adapter.drbd;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DrbdRscDfnData extends BaseTransactionObject implements DrbdRscDfnObject
{
    final ResourceDefinition rscDfn;
    final TcpPortNumber port;
    final TransportType transportType;
    final String secret;
    final List<DrbdRscData> drbdRscDataList;

    public DrbdRscDfnData(
        ResourceDefinition rscDfnRef,
        TcpPortNumber portRef,
        TransportType transportTypeRef,
        String secretRef,
        List<DrbdRscData> drbdRscDataListRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        rscDfn = Objects.requireNonNull(rscDfnRef);
        port = Objects.requireNonNull(portRef);
        transportType = Objects.requireNonNull(transportTypeRef);
        secret = Objects.requireNonNull(secretRef);
        drbdRscDataList = Collections.unmodifiableList(drbdRscDataListRef);

        transObjs = Arrays.asList(
            // FIXME: DevMgrRework: fill transObjs
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.DRBD;
    }

    @Override
    public ResourceDefinition getResourceDefinition()
    {
        return rscDfn;
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
