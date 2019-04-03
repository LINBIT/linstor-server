package com.linbit.linstor.storage;

import com.linbit.linstor.Resource;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSet;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.annotation.Nullable;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class AbsRscData<VLM_TYPE extends VlmProviderObject>
    extends BaseTransactionObject implements RscLayerObject
{
    // unmodifiable data
    protected final int rscLayerId;
    protected final Resource rsc;
    protected final String rscSuffix;
    protected final ResourceLayerIdDatabaseDriver dbDriver;

    // persisted, serialized
    protected final TransactionMap<VolumeNumber, VLM_TYPE> vlmMap;
    protected final TransactionSimpleObject<AbsRscData<VLM_TYPE>, RscLayerObject> parent;
    protected final TransactionSet<AbsRscData<VLM_TYPE>, RscLayerObject> children;

    // volatile satellite only
    private boolean checkFileSystem;

    public AbsRscData(
        int rscLayerIdRef,
        Resource rscRef,
        @Nullable RscLayerObject parentRef,
        Set<RscLayerObject> childrenRef,
        String rscNameSuffixRef,
        ResourceLayerIdDatabaseDriver dbDriverRef,
        Map<VolumeNumber, VLM_TYPE> vlmProviderObjectsRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef
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
    public RscLayerObject getParent()
    {
        return parent.get();
    }

    @Override
    public void setParent(RscLayerObject parentRscLayerObjectRef) throws SQLException
    {
        parent.set(parentRscLayerObjectRef);
    }

    @Override
    public Set<RscLayerObject> getChildren()
    {
        return children;
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
    public Map<VolumeNumber, VLM_TYPE> getVlmLayerObjects()
    {
        return vlmMap;
    }

    @Override
    public void remove(VolumeNumber vlmNrRef) throws SQLException
    {
        for (RscLayerObject rscLayerObject : children)
        {
            rscLayerObject.remove(vlmNrRef);
        }
        VLM_TYPE vlm = vlmMap.remove(vlmNrRef);
        deleteVlmFromDatabase(vlm);
    }

    protected abstract void deleteVlmFromDatabase(VLM_TYPE vlm) throws SQLException;

    protected abstract void deleteRscFromDatabase() throws SQLException;

    @Override
    public void delete() throws SQLException
    {
        for (RscLayerObject rscLayerObject : children)
        {
            rscLayerObject.delete();
        }
        deleteRscFromDatabase();
        dbDriver.delete(this);
    }

    protected List<RscLayerDataApi> getChildrenPojos(AccessContext accCtx) throws AccessDeniedException
    {
        List<RscLayerDataApi> childrenPojos = new ArrayList<>();
        for (RscLayerObject rscLayerObject : children)
        {
            childrenPojos.add(rscLayerObject.asPojo(accCtx));
        }
        return childrenPojos;
    }

    public boolean checkFileSystem()
    {
        return checkFileSystem;
    }

    public void disableCheckFileSystem()
    {
        this.checkFileSystem = false;
    }
}
