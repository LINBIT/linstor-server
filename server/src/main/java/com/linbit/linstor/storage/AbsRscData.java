package com.linbit.linstor.storage;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSet;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.ArrayList;
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
    protected final ResourceLayerIdDatabaseDriver dbDriver;

    // persisted, serialized
    protected final TransactionMap<VolumeNumber, VLM_TYPE> vlmMap;
    protected final TransactionSimpleObject<AbsRscData<RSC, VLM_TYPE>, AbsRscLayerObject<RSC>> parent;
    protected final TransactionSet<AbsRscData<RSC, VLM_TYPE>, AbsRscLayerObject<RSC>> children;
    protected final TransactionSimpleObject<AbsRscData<RSC, VLM_TYPE>, Boolean> suspend;

    // volatile satellite only
    private boolean checkFileSystem;

    public AbsRscData(
        int rscLayerIdRef,
        RSC rscRef,
        @Nullable AbsRscLayerObject<RSC> parentRef,
        Set<AbsRscLayerObject<RSC>> childrenRef,
        String rscNameSuffixRef,
        ResourceLayerIdDatabaseDriver dbDriverRef,
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
        vlmMap = transObjFactory.createTransactionMap(vlmProviderObjectsRef, null);
        suspend = transObjFactory.createTransactionSimpleObject(this, false, dbDriverRef.getSuspendDriver());

        checkFileSystem = true;

        transObjs = new ArrayList<>();
        transObjs.add(rsc);
        transObjs.add(parent);
        transObjs.add(children);
        transObjs.add(vlmMap);
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
    public void setParent(AbsRscLayerObject<RSC> parentRscLayerObjectRef) throws DatabaseException
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
    public void remove(AccessContext accCtx, VolumeNumber vlmNrRef)
        throws DatabaseException, AccessDeniedException
    {
        for (AbsRscLayerObject<RSC> rscLayerObject : children)
        {
            rscLayerObject.remove(accCtx, vlmNrRef);
        }
        VLM_TYPE vlmData = vlmMap.remove(vlmNrRef);
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

    protected abstract void deleteVlmFromDatabase(VLM_TYPE vlm) throws DatabaseException;

    protected abstract void deleteRscFromDatabase() throws DatabaseException;

    @Override
    public void delete() throws DatabaseException
    {
        for (AbsRscLayerObject<RSC> rscLayerObject : children)
        {
            rscLayerObject.delete();
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
    public void setSuspendIo(boolean suspendRef) throws DatabaseException
    {
        suspend.set(suspendRef);
    }

    @Override
    public boolean getSuspendIo()
    {
        return suspend.get();
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
}
