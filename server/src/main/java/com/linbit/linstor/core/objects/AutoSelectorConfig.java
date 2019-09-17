package com.linbit.linstor.core.objects;

import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class AutoSelectorConfig extends BaseTransactionObject implements DbgInstanceUuid, ProtectedObject
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
    private final TransactionSimpleObject<ResourceGroupData, Boolean> disklessOnRemaining;

    public AutoSelectorConfig(
        ResourceGroupData rscGrpRef,
        Integer replicaCountRef,
        String storPoolNameRef,
        List<String> doNotPlaceWithRscListRef,
        String doNotPlaceWithRscRegexRef,
        List<String> replicasOnSameListRef,
        List<String> replicasOnDifferentListRef,
        List<DeviceLayerKind> layerStackRef,
        List<DeviceProviderKind> allowedProviderListRef,
        Boolean disklessOnRemainingRef,
        ResourceGroupDataDatabaseDriver dbDriverRef,
        TransactionObjectFactory transactionObjectFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        rscGrp = rscGrpRef;
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
        layerStack = transactionObjectFactoryRef.createTransactionPrimitiveList(
            rscGrpRef,
            layerStackRef,
            dbDriverRef.getLayerStackDriver()
        );
        allowedProviderList = transactionObjectFactoryRef.createTransactionPrimitiveList(
            rscGrpRef,
            allowedProviderListRef,
            dbDriverRef.getAllowedProviderListDriver()
        );
        disklessOnRemaining = transactionObjectFactoryRef.createTransactionSimpleObject(
            rscGrpRef,
            disklessOnRemainingRef,
            dbDriverRef.getDisklessOnRemainingDriver()
        );
        transObjs = Arrays.asList(
            replicaCount,
            storPoolName,
            doNotPlaceWithRscList,
            doNotPlaceWithRscRegex,
            replicasOnSameList,
            replicasOnDifferentList,
            layerStack,
            allowedProviderList
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


    public ResourceGroup getResourceGroup()
    {
        return rscGrp;
    }


    public Integer getReplicaCount(AccessContext accCtx) throws AccessDeniedException
    {
        getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return replicaCount.get();
    }


    public String getStorPoolNameStr(AccessContext accCtx) throws AccessDeniedException
    {
        getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return storPoolName.get();
    }


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


    public String getDoNotPlaceWithRscRegex(AccessContext accCtx) throws AccessDeniedException
    {
        return doNotPlaceWithRscRegex.get();
    }


    public List<String> getReplicasOnSameList(AccessContext accCtx) throws AccessDeniedException
    {
        return replicasOnSameList;
    }


    public List<String> getReplicasOnDifferentList(AccessContext accCtx) throws AccessDeniedException
    {
        return replicasOnDifferentList;
    }


    public List<DeviceLayerKind> getLayerStackList(AccessContext accCtx) throws AccessDeniedException
    {
        return layerStack;
    }


    public List<DeviceProviderKind> getProviderList(AccessContext accCtx) throws AccessDeniedException
    {
        return allowedProviderList;
    }


    public Boolean getDisklessOnRemaining(AccessContext accCtxRef) throws AccessDeniedException
    {
        return disklessOnRemaining.get();
    }


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
            Collections.unmodifiableList(allowedProviderList),
            disklessOnRemaining.get()
        );
    }

    public void applyChanges(AutoSelectFilterApi autoPlaceConfigRef) throws DatabaseException
    {
        if (autoPlaceConfigRef != null)
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
            Boolean disklessOnRemainingRef = autoPlaceConfigRef.getDisklessOnRemaining();
            if (disklessOnRemainingRef != null)
            {
                disklessOnRemaining.set(disklessOnRemainingRef);
            }
            List<String> doNotPlaceWithRscListRef = autoPlaceConfigRef.getDoNotPlaceWithRscList();
            if (doNotPlaceWithRscListRef != null && !doNotPlaceWithRscListRef.isEmpty())
            {
                doNotPlaceWithRscList.clear();
                doNotPlaceWithRscList.addAll(doNotPlaceWithRscListRef);
            }
            if (autoPlaceConfigRef.getDoNotPlaceWithRscRegex() != null)
            {
                doNotPlaceWithRscRegex.set(autoPlaceConfigRef.getDoNotPlaceWithRscRegex());
            }
            if (autoPlaceConfigRef.getLayerStackList() != null &&
                !autoPlaceConfigRef.getLayerStackList().equals(layerStack))
            {
                layerStack.clear();
                layerStack.addAll(autoPlaceConfigRef.getLayerStackList());
            }
            List<String> replicasOnSameListRef = autoPlaceConfigRef.getReplicasOnSameList();
            if (replicasOnSameListRef != null && !replicasOnSameListRef.isEmpty())
            {
                replicasOnSameList.clear();
                replicasOnSameList.addAll(replicasOnSameListRef);
            }
            List<String> replicasOnDifferentListRef = autoPlaceConfigRef.getReplicasOnDifferentList();
            if (replicasOnDifferentListRef != null && !replicasOnDifferentListRef.isEmpty())
            {
                replicasOnDifferentList.clear();
                replicasOnDifferentList.addAll(replicasOnDifferentListRef);
            }
        }
    }
}
