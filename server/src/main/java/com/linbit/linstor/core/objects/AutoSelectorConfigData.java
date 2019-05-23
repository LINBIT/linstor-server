package com.linbit.linstor.core.objects;

import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class AutoSelectorConfigData extends BaseTransactionObject implements AutoSelectorConfig
{
    private final UUID dbgInstanceId;
    private final ResourceGroupData rscGrp;

    private final TransactionSimpleObject<ResourceGroupData, Integer> replicaCount;
    private final TransactionSimpleObject<ResourceGroupData, String> storPoolName;
    private final TransactionList<ResourceGroupData, String> doNotPlaceWithRscList;
    private final TransactionSimpleObject<ResourceGroupData, String> doNotPlaceWithRscRegex;
    private final TransactionList<ResourceGroupData, String> replicasOnSameList;
    private final TransactionList<ResourceGroupData, String> replicasOnDifferentList;
    private final TransactionList<ResourceGroupData, DeviceLayerKind> layerStack;
    private final TransactionList<ResourceGroupData, DeviceProviderKind> allowedProviderList;

    public AutoSelectorConfigData(
        ResourceGroupData rscGrpRef,
        Integer replicaCountRef,
        String storPoolNameRef,
        List<String> doNotPlaceWithRscListRef,
        String doNotPlaceWithRscRegexRef,
        List<String> replicasOnSameListRef,
        List<String> replicasOnDifferentListRef,
        TransactionList<ResourceGroupData, DeviceLayerKind> layerStackRef,
        List<DeviceProviderKind> allowedProviderListRef,
        ResourceGroupDataDatabaseDriver dbDriverRef,
        TransactionObjectFactory transactionObjectFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        rscGrp = rscGrpRef;
        layerStack = layerStackRef;
        dbgInstanceId = UUID.randomUUID();

        replicaCount = transactionObjectFactoryRef.createTransactionSimpleObject(
            rscGrpRef,
            replicaCountRef,
            dbDriverRef.getReplicaCountDriver()
        );
        storPoolName = transactionObjectFactoryRef.createTransactionSimpleObject(
            rscGrpRef,
            storPoolNameRef,
            dbDriverRef.getStorPoolNameDriver()
        );
        doNotPlaceWithRscList = transactionObjectFactoryRef.createTransactionPrimitiveList(
            rscGrpRef,
            doNotPlaceWithRscListRef,
            dbDriverRef.getDoNotPlaceWithRscListDriver()
        );
        doNotPlaceWithRscRegex = transactionObjectFactoryRef.createTransactionSimpleObject(
            rscGrpRef,
            doNotPlaceWithRscRegexRef,
            dbDriverRef.getDoNotPlaceWithRscRegexDriver()
        );
        replicasOnSameList = transactionObjectFactoryRef.createTransactionPrimitiveList(
            rscGrpRef,
            replicasOnSameListRef,
            dbDriverRef.getReplicasOnSameListDriver()
        );
        replicasOnDifferentList = transactionObjectFactoryRef.createTransactionPrimitiveList(
            rscGrpRef,
            replicasOnDifferentListRef,
            dbDriverRef.getReplicasOnDifferentDriver()
        );
        allowedProviderList = transactionObjectFactoryRef.createTransactionPrimitiveList(
            rscGrpRef,
            allowedProviderListRef,
            dbDriverRef.getAllowedProviderListDriver()
        );
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return rscGrp.getObjProt();
    }

    @Override
    public ResourceGroup getResourceGroup()
    {
        return rscGrp;
    }

    @Override
    public Integer getReplicaCount(AccessContext accCtx) throws AccessDeniedException
    {
        getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return replicaCount.get();
    }

    @Override
    public String getStorPoolNameStr(AccessContext accCtx) throws AccessDeniedException
    {
        getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return storPoolName.get();
    }

    @Override
    public List<String> getDoNotPlaceWithRscList(AccessContext accCtx) throws AccessDeniedException
    {
        ObjectProtection objProt = getObjProt();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        List<String> ret;
        AccessType queryAccess = objProt.queryAccess(accCtx);

        if (queryAccess.hasAccess(AccessType.CHANGE))
        {
            ret = doNotPlaceWithRscList;
        }
        else
        {
            ret = Collections.unmodifiableList(doNotPlaceWithRscList);
        }
        return ret;
    }

    @Override
    public String getDoNotPlaceWithRscRegex(AccessContext accCtx) throws AccessDeniedException
    {
        return doNotPlaceWithRscRegex.get();
    }

    @Override
    public List<String> getReplicasOnSameList(AccessContext accCtx) throws AccessDeniedException
    {
        return replicasOnSameList;
    }

    @Override
    public List<String> getReplicasOnDifferentList(AccessContext accCtx) throws AccessDeniedException
    {
        return replicasOnDifferentList;
    }

    @Override
    public List<DeviceLayerKind> getLayerStackList(AccessContext accCtx) throws AccessDeniedException
    {
        return layerStack;
    }

    @Override
    public List<DeviceProviderKind> getProviderList(AccessContext accCtx) throws AccessDeniedException
    {
        return allowedProviderList;
    }

    @Override
    public AutoSelectFilterApi getApiData(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new AutoSelectFilterPojo(
            replicaCount.get(),
            storPoolName.get(),
            Collections.unmodifiableList(doNotPlaceWithRscList),
            doNotPlaceWithRscRegex.get(),
            Collections.unmodifiableList(replicasOnSameList),
            Collections.unmodifiableList(replicasOnDifferentList),
            Collections.unmodifiableList(layerStack),
            Collections.unmodifiableList(allowedProviderList)
        );
    }

    public void applyChanges(AutoSelectFilterApi autoPlaceConfigRef) throws DatabaseException
    {
        if (autoPlaceConfigRef.getReplicaCount() != null)
        {
            replicaCount.set(autoPlaceConfigRef.getReplicaCount());
        }
        String pojoStorPool = autoPlaceConfigRef.getStorPoolNameStr();
        if (pojoStorPool != null)
        {
            if (pojoStorPool.equals(""))
            {
                storPoolName.set(null);
            }
            else
            {
                storPoolName.set(pojoStorPool);
            }
        }
        if (!autoPlaceConfigRef.getDoNotPlaceWithRscList().isEmpty())
        {
            doNotPlaceWithRscList.clear();
            doNotPlaceWithRscList.addAll(autoPlaceConfigRef.getDoNotPlaceWithRscList());
        }
        if (autoPlaceConfigRef.getDoNotPlaceWithRscRegex() != null)
        {
            doNotPlaceWithRscRegex.set(autoPlaceConfigRef.getDoNotPlaceWithRscRegex());
        }
        if (!autoPlaceConfigRef.getReplicasOnSameList().isEmpty())
        {
            replicasOnSameList.clear();
            replicasOnSameList.addAll(autoPlaceConfigRef.getReplicasOnSameList());
        }
        if (!autoPlaceConfigRef.getReplicasOnDifferentList().isEmpty())
        {
            replicasOnDifferentList.clear();
            replicasOnDifferentList.addAll(autoPlaceConfigRef.getReplicasOnDifferentList());
        }
    }
}
