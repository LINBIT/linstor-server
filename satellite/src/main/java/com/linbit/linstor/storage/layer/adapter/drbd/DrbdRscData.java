package com.linbit.linstor.storage.layer.adapter.drbd;

import com.linbit.linstor.NodeId;
import com.linbit.linstor.Resource;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DrbdRscData extends BaseTransactionObject implements DrbdRscObject
{
    final Resource rsc;
    final String rscSuffix;
    final @Nullable RscLayerObject parent;
    final List<RscLayerObject> children;
    final DrbdRscDfnData drbdRscDfnData;
    final NodeId nodeId;
    final boolean disklessForPeers;
    final boolean diskless;

    transient boolean failed = false;
    transient boolean exists = false;
    transient boolean requiresAdjust = false;
    transient boolean isPrimary;
    transient boolean isSuspended = false;

    final transient Map<VolumeNumber, DrbdVlmData> vlmStates;

    public DrbdRscData(
        Resource rscRef,
        String rscSuffixRef,
        @Nullable RscLayerObject parentRef,
        DrbdRscDfnData drbdRscDfnDataRef,
        List<RscLayerObject> childrenRef,
        Map<VolumeNumber, DrbdVlmData> vlmLayerObjectsMapRef,
        NodeId nodeIdRef,
        boolean disklessForPeersRef,
        boolean disklessRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);

        nodeId = nodeIdRef;
        disklessForPeers = disklessForPeersRef;
        diskless = disklessRef;
        rscSuffix = rscSuffixRef == null ? "" : rscSuffixRef;

        rsc = Objects.requireNonNull(rscRef);
        parent = parentRef;
        drbdRscDfnData = Objects.requireNonNull(drbdRscDfnDataRef);
        children = Collections.unmodifiableList(childrenRef);
        vlmStates = Collections.unmodifiableMap(vlmLayerObjectsMapRef);

        transObjs = Arrays.asList(
            // FIXME: DevMgrRework: fill transObjs
        );
    }

    @Override
    public Resource getResource()
    {
        return rsc;
    }

    @Override
    public String getResourceNameSuffix()
    {
        return rscSuffix;
    }

    @Override
    public boolean isFailed()
    {
        return failed;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.DRBD;
    }

    @Override
    public @Nullable RscLayerObject getParent()
    {
        return parent;
    }

    @Override
    public List<RscLayerObject> getChildren()
    {
        return children;
    }

    @Override
    public NodeId getNodeId()
    {
        return nodeId;
    }

    @Override
    public boolean isDiskless()
    {
        return diskless;
    }

    @Override
    public boolean isDisklessForPeers()
    {
        return disklessForPeers;
    }

    @Override
    public @Nullable DrbdRscDfnData getRscDfnLayerObject()
    {
        return drbdRscDfnData;
    }

    @Override
    public Map<VolumeNumber, DrbdVlmData> getVlmLayerObjects()
    {
        return vlmStates;
    }
}
