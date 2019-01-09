package com.linbit.linstor.storage.layer.provider;

import com.linbit.linstor.Resource;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.storage.interfaces.categories.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StorageRscData extends BaseTransactionObject implements RscLayerObject
{
    private final RscLayerObject parent;
    private final Resource rsc;
    private final String rscNameSuffix;
    private final Map<VolumeNumber, VlmProviderObject> vlmProviderObjects;

    public StorageRscData(
        RscLayerObject parentRef,
        Resource rscRef,
        String rscNameSuffixRef,
        Map<VolumeNumber,
        VlmProviderObject> vlmProviderObjectsRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        parent = parentRef;
        rsc = Objects.requireNonNull(rscRef);
        rscNameSuffix = rscNameSuffixRef == null ? "" : rscNameSuffixRef;
        vlmProviderObjects = Collections.unmodifiableMap(vlmProviderObjectsRef);

        transObjs = Arrays.asList(
            // FIXME: DevMgrRework: fill transObjs
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public RscLayerObject getParent()
    {
        return parent;
    }

    @Override
    public List<RscLayerObject> getChildren()
    {
        return Collections.emptyList();
    }

    @Override
    public Resource getResource()
    {
        return rsc;
    }

    @Override
    public String getResourceNameSuffix()
    {
        return rscNameSuffix;
    }

    @Override
    public RscDfnLayerObject getRscDfnLayerObject()
    {
        return null;
    }

    @Override
    public Map<VolumeNumber, VlmProviderObject> getVlmLayerObjects()
    {
        return vlmProviderObjects;
    }
}
