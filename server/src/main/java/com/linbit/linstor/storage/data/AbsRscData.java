package com.linbit.linstor.storage.data;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSet;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class AbsRscData<RSC extends AbsResource<RSC>, VLM_TYPE extends VlmProviderObject<RSC>>
    extends BaseTransactionObject implements AbsRscLayerObject<RSC>
{
    // unmodifiable data
    protected final int rscLayerId;
    protected final RSC rsc;
    protected final String rscSuffix;
    protected final LayerResourceIdDatabaseDriver dbDriver;

    // not (explicitly) persisted, serialized
    protected final TransactionMap<AbsRscData<RSC, VLM_TYPE>, VolumeNumber, VLM_TYPE> vlmMap;
    protected final TransactionSet<AbsRscData<RSC, VLM_TYPE>, AbsRscLayerObject<RSC>> children;

    // not persisted, serialized
    protected final TransactionSimpleObject<AbsRscData<RSC, VLM_TYPE>, Set<LayerIgnoreReason>> ignoreReasons;

    // persisted, serialized
    protected final TransactionSimpleObject<AbsRscData<RSC, VLM_TYPE>, AbsRscLayerObject<RSC>> parent;
    // TODO: this would make more sense of VLM level since non-DRBD layers can now also be suspended
    protected final TransactionSimpleObject<AbsRscData<RSC, VLM_TYPE>, Boolean> suspend;

    // volatile satellite only
    private boolean checkFileSystem;
    private @Nullable Boolean isSuspended = null; // unknown

    private final Map<AbsRscLayerObject<?>, Boolean> clonePassthroughModeMap = new HashMap<>(); // not cloning if empty

    public AbsRscData(
        int rscLayerIdRef,
        RSC rscRef,
        @Nullable AbsRscLayerObject<RSC> parentRef,
        Set<AbsRscLayerObject<RSC>> childrenRef,
        @Nullable String rscNameSuffixRef,
        LayerResourceIdDatabaseDriver dbDriverRef,
        Map<VolumeNumber, VLM_TYPE> vlmProviderObjectsRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        rscLayerId = rscLayerIdRef;
        dbDriver = dbDriverRef;
        rsc = Objects.requireNonNull(rscRef);
        rscSuffix = rscNameSuffixRef == null ? "" : rscNameSuffixRef;

        parent = transObjFactory.createTransactionSimpleObject(this, parentRef, dbDriverRef.getParentDriver());
        children = transObjFactory.createTransactionSet(this, childrenRef, null);
        vlmMap = transObjFactory.createTransactionMap(this, vlmProviderObjectsRef, null);
        suspend = transObjFactory.createTransactionSimpleObject(this, false, dbDriverRef.getSuspendDriver());
        ignoreReasons = transObjFactory.createTransactionSimpleObject(
            this,
            EnumSet.noneOf(LayerIgnoreReason.class),
            null
        );

        checkFileSystem = true;

        transObjs = new ArrayList<>();
        transObjs.add(rsc);
        transObjs.add(parent);
        transObjs.add(children);
        transObjs.add(vlmMap);
        transObjs.add(suspend);
        transObjs.add(ignoreReasons);
    }

    @Override
    public int getRscLayerId()
    {
        return rscLayerId;
    }

    @Override
    public AbsRscLayerObject<RSC> getParent()
    {
        return parent.get();
    }

    @Override
    public void setParent(@Nullable AbsRscLayerObject<RSC> parentRscLayerObjectRef) throws DatabaseException
    {
        parent.set(parentRscLayerObjectRef);
    }

    @Override
    public Set<AbsRscLayerObject<RSC>> getChildren()
    {
        return children;
    }

    @Override
    public RSC getAbsResource()
    {
        return rsc;
    }

    @Override
    public String getResourceNameSuffix()
    {
        return rscSuffix;
    }

    @Override
    public Map<VolumeNumber, VLM_TYPE> getVlmLayerObjects()
    {
        return vlmMap;
    }

    @Override
    public boolean exists()
    {
        boolean exists = true;
        for (VLM_TYPE vlm : vlmMap.values())
        {
            if (!vlm.exists())
            {
                exists = false;
                break;
            }
        }
        return exists;
    }

    @Override
    public Boolean isSuspended()
    {
        return isSuspended;
    }

    @Override
    public void setIsSuspended(boolean isSuspendedRef)
    {
        isSuspended = isSuspendedRef;
    }

    @Override
    public void remove(AccessContext accCtx, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        for (AbsRscLayerObject<RSC> rscLayerObject : children)
        {
            rscLayerObject.remove(accCtx, vlmNrRef);
        }
        VLM_TYPE vlmData = vlmMap.remove(vlmNrRef);
        if (vlmData != null)
        {
            /*
             * vlmData might be null when having mixed external and internal drbd
             * metadata will result in drbd-resource with i.e. 2 drbd-volumes and
             * 1 storage child-rsc. this storage child-rsc will only have 1
             * storage-volume (one external drbd-volume, the other has internal)
             */
            if (!(vlmData instanceof VlmLayerObject))
            {
                StorPool storPool = vlmData.getStorPool();
                if (vlmData.getVolume() instanceof Volume)
                {
                    storPool.removeVolume(accCtx, (VlmProviderObject<Resource>) vlmData);
                }
                else
                {
                    storPool.removeSnapshotVolume(accCtx, (VlmProviderObject<Snapshot>) vlmData);
                }
            }
            deleteVlmFromDatabase(vlmData);
        }
    }

    protected abstract void deleteVlmFromDatabase(VLM_TYPE vlm) throws DatabaseException;

    protected abstract void deleteRscFromDatabase() throws DatabaseException;

    @Override
    public void delete(AccessContext accCtx) throws DatabaseException, AccessDeniedException
    {
        for (AbsRscLayerObject<RSC> rscLayerObject : children)
        {
            rscLayerObject.delete(accCtx);
        }
        // copy to avoid concurrentModificationException
        List<VolumeNumber> vlmNrsToRemove = new ArrayList<>(vlmMap.keySet());
        for (VolumeNumber vlmNr : vlmNrsToRemove)
        {
            remove(accCtx, vlmNr);
        }
        deleteRscFromDatabase();
        dbDriver.delete(this);
    }

    protected List<RscLayerDataApi> getChildrenPojos(AccessContext accCtx) throws AccessDeniedException
    {
        List<RscLayerDataApi> childrenPojos = new ArrayList<>();
        for (AbsRscLayerObject<RSC> rscLayerObject : children)
        {
            childrenPojos.add(rscLayerObject.asPojo(accCtx));
        }
        return childrenPojos;
    }

    @Override
    public void setShouldSuspendIo(boolean suspendRef) throws DatabaseException
    {
        suspend.set(suspendRef);
    }

    @Override
    public boolean getShouldSuspendIo()
    {
        return suspend.get();
    }

    @Override
    public Set<LayerIgnoreReason> getIgnoreReasons()
    {
        return ignoreReasons.get();
    }

    @Override
    public boolean addAllIgnoreReasons(Set<LayerIgnoreReason> ignoreReasonsRef)
    {
        return ignoreReasons.get().addAll(ignoreReasonsRef);
    }

    @Override
    public boolean addIgnoreReasons(LayerIgnoreReason... ignoreReasonsRef) throws DatabaseException
    {
        return ignoreReasons.get().addAll(Arrays.asList(ignoreReasonsRef));
    }

    @Override
    public void clearIgnoreReasons() throws DatabaseException
    {
        ignoreReasons.get().clear();
    }

    @Override
    public boolean checkFileSystem()
    {
        return checkFileSystem;
    }

    @Override
    public void disableCheckFileSystem()
    {
        this.checkFileSystem = false;
    }

    @Override
    public void setClonePassthroughMode(AbsRscLayerObject<?> targetLayerDataRef, boolean targetHasPassthrough)
    {
        clonePassthroughModeMap.put(targetLayerDataRef, targetHasPassthrough);
    }

    @Override
    public void removeClonePassthroughMode(AbsRscLayerObject<?> targetLayerDataRef)
    {
        clonePassthroughModeMap.remove(targetLayerDataRef);
    }

    @Override
    public Boolean isClonePassthroughMode()
    {
        return !clonePassthroughModeMap.isEmpty() && clonePassthroughModeMap.values().stream()
            .allMatch(Boolean.TRUE::equals);
    }

    @Override
    public void cleanupAfterCloneFinished()
    {
        clonePassthroughModeMap.clear();
        for (VLM_TYPE vlmData : vlmMap.values())
        {
            vlmData.setCloneDevicePath(null);
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + rscLayerId;
        result = prime * result + rscSuffix.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean ret = obj instanceof AbsRscData;
        if (ret)
        {
            AbsRscData<?, ?> other = (AbsRscData<?, ?>) obj;
            ret = Objects.equals(rscSuffix, other.rscSuffix) &&
                Objects.equals(rscLayerId, other.rscLayerId);
        }
        return ret;
    }

}
