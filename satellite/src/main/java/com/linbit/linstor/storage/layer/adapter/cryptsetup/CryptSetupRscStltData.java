package com.linbit.linstor.storage.layer.adapter.cryptsetup;

import com.linbit.linstor.Resource;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.storage.interfaces.categories.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class CryptSetupRscStltData extends BaseTransactionObject implements RscLayerObject
{
    final Map<VolumeNumber, CryptSetupVlmStltData> unmodVlmLayerObjects;
    final String rscNameSuffix;
    final Resource rsc;
    final List<RscLayerObject> unmodChildren;

    final @Nullable RscLayerObject parent;

    public CryptSetupRscStltData(
        Resource rscRef,
        String rscNameSuffixRef,
        @Nullable RscLayerObject parentRef,
        List<RscLayerObject> children,
        Map<VolumeNumber, CryptSetupVlmStltData> vlmLayerObjectsRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        rsc = Objects.requireNonNull(rscRef);
        rscNameSuffix = Optional.ofNullable(rscNameSuffixRef).orElse("");
        parent = parentRef;
        unmodVlmLayerObjects = Collections.unmodifiableMap(vlmLayerObjectsRef);
        unmodChildren = Collections.unmodifiableList(children);
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.CRYPT_SETUP;
    }

    @Override
    public @Nullable RscLayerObject getParent()
    {
        return parent;
    }

    @Override
    public List<RscLayerObject> getChildren()
    {
        return unmodChildren;
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
    public @Nullable RscDfnLayerObject getRscDfnLayerObject()
    {
        return null;
    }

    @Override
    public Map<VolumeNumber, ? extends VlmLayerObject> getVlmLayerObjects()
    {
        return unmodVlmLayerObjects;
    }
}
