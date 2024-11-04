package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class AutoSelectorConfig extends BaseTransactionObject
    implements DbgInstanceUuid, ProtectedObject
{
    private final UUID dbgInstanceId;
    private final ResourceGroup rscGrp;

    private final TransactionSimpleObject<ResourceGroup, Integer> replicaCount;
    private final TransactionList<ResourceGroup, String> nodeNameList;
    private final TransactionList<ResourceGroup, String> storPoolNameList;
    private final TransactionList<ResourceGroup, String> storPoolDisklessNameList;
    private final TransactionList<ResourceGroup, String> doNotPlaceWithRscList;
    private final TransactionSimpleObject<ResourceGroup, String> doNotPlaceWithRscRegex;
    private final TransactionList<ResourceGroup, String> replicasOnSameList;
    private final TransactionList<ResourceGroup, String> replicasOnDifferentList;
    private final TransactionMap<ResourceGroup, String, Integer> xReplicasOnDifferentMap;
    private final TransactionList<ResourceGroup, DeviceLayerKind> layerStack;
    private final TransactionList<ResourceGroup, DeviceProviderKind> allowedProviderList;
    private final TransactionSimpleObject<ResourceGroup, Boolean> disklessOnRemaining;

    public AutoSelectorConfig(
        ResourceGroup rscGrpRef,
        @Nullable Integer replicaCountRef,
        @Nullable List<String> nodeNameListRef,
        @Nullable List<String> storPoolNameListRef,
        @Nullable List<String> storPoolDisklessNameListRef,
        @Nullable List<String> doNotPlaceWithRscListRef,
        @Nullable String doNotPlaceWithRscRegexRef,
        @Nullable List<String> replicasOnSameListRef,
        @Nullable List<String> replicasOnDifferentListRef,
        @Nullable Map<String, Integer> xReplicasOnDifferentMapRef,
        @Nullable List<DeviceLayerKind> layerStackRef,
        @Nullable List<DeviceProviderKind> allowedProviderListRef,
        @Nullable Boolean disklessOnRemainingRef,
        ResourceGroupDatabaseDriver dbDriverRef,
        TransactionObjectFactory transactionObjectFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        rscGrp = rscGrpRef;
        dbgInstanceId = UUID.randomUUID();

        replicaCount = transactionObjectFactoryRef.createTransactionSimpleObject(
            rscGrpRef,
            replicaCountRef == null ? 2 : replicaCountRef,
            dbDriverRef.getReplicaCountDriver()
        );
        nodeNameList = transactionObjectFactoryRef.createTransactionPrimitiveList(
            rscGrpRef,
            nodeNameListRef,
            dbDriverRef.getNodeNameDriver()
        );
        storPoolNameList = transactionObjectFactoryRef.createTransactionPrimitiveList(
            rscGrpRef,
            storPoolNameListRef,
            dbDriverRef.getStorPoolNameDriver()
        );
        storPoolDisklessNameList = transactionObjectFactoryRef.createTransactionPrimitiveList(
            rscGrpRef,
            storPoolDisklessNameListRef,
            dbDriverRef.getStorPoolDisklessNameDriver()
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
        xReplicasOnDifferentMap = transactionObjectFactoryRef.createTransactionPrimitiveMap(
            rscGrpRef,
            xReplicasOnDifferentMapRef,
            dbDriverRef.getXReplicasOnDifferentMapDriver()
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
            nodeNameList,
            storPoolNameList,
            storPoolDisklessNameList,
            doNotPlaceWithRscList,
            doNotPlaceWithRscRegex,
            replicasOnSameList,
            replicasOnDifferentList,
            xReplicasOnDifferentMap,
            layerStack,
            allowedProviderList,
            disklessOnRemaining
        );
    }

    public AutoSelectorConfig(
        AutoSelectorConfig baseCfg,
        AutoSelectFilterApi priorityApi,
        ResourceGroupDatabaseDriver driver,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        this(
            baseCfg.rscGrp,
            get(priorityApi.getReplicaCount(), baseCfg.replicaCount),
            get(priorityApi.getNodeNameList(), baseCfg.nodeNameList),
            get(priorityApi.getStorPoolNameList(), baseCfg.storPoolNameList),
            get(priorityApi.getStorPoolDisklessNameList(), baseCfg.storPoolDisklessNameList),
            get(priorityApi.getDoNotPlaceWithRscList(), baseCfg.doNotPlaceWithRscList),
            get(priorityApi.getDoNotPlaceWithRscRegex(), baseCfg.doNotPlaceWithRscRegex),
            get(priorityApi.getReplicasOnSameList(), baseCfg.replicasOnSameList),
            get(priorityApi.getReplicasOnDifferentList(), baseCfg.replicasOnDifferentList),
            get(priorityApi.getXReplicasOnDifferentMap(), baseCfg.xReplicasOnDifferentMap),
            get(priorityApi.getLayerStackList(), baseCfg.layerStack),
            get(priorityApi.getProviderList(), baseCfg.allowedProviderList),
            get(priorityApi.getDisklessOnRemaining(), baseCfg.disklessOnRemaining),
            driver,
            transObjFactory,
            transMgrProvider
        );
    }

    private static <T> T get(T value, TransactionSimpleObject<?, T> dflt)
    {
        return value == null ? dflt.get() : value;
    }

    private static <T> List<T> get(List<T> value, TransactionList<?, T> dflt)
    {
        return value == null ? dflt : new ArrayList<>(value);
    }

    private static <K, V> Map<K, V> get(Map<K, V> value, TransactionMap<?, K, V> dflt)
    {
        return value == null ? dflt : new HashMap<>(value);
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

    public int getReplicaCount(AccessContext accCtx) throws AccessDeniedException
    {
        getObjProt().requireAccess(accCtx, AccessType.VIEW);
        @Nullable Integer replCt = replicaCount.get();
        if (replCt == null)
        {
            throw new ImplementationError("replCt should not be nullable here!");
        }
        return replCt;
    }

    public List<String> getNodeNameList(AccessContext accCtx) throws AccessDeniedException
    {
        return protectedList(accCtx, nodeNameList);
    }

    public List<String> getStorPoolNameList(AccessContext accCtx) throws AccessDeniedException
    {
        return protectedList(accCtx, storPoolNameList);
    }

    public List<String> getStorPoolDisklessNameList(AccessContext accCtx) throws AccessDeniedException
    {
        return protectedList(accCtx, storPoolDisklessNameList);
    }

    public List<String> getDoNotPlaceWithRscList(AccessContext accCtx) throws AccessDeniedException
    {
        return protectedList(accCtx, doNotPlaceWithRscList);
    }

    public String getDoNotPlaceWithRscRegex(AccessContext accCtx) throws AccessDeniedException
    {
        return doNotPlaceWithRscRegex.get();
    }

    public List<String> getReplicasOnSameList(AccessContext accCtx) throws AccessDeniedException
    {
        return protectedList(accCtx, replicasOnSameList);
    }

    public List<String> getReplicasOnDifferentList(AccessContext accCtx) throws AccessDeniedException
    {
        return protectedList(accCtx, replicasOnDifferentList);
    }

    public Map<String, Integer> getXReplicasOnDifferentMap(AccessContext accCtx) throws AccessDeniedException
    {
        return protectedMap(accCtx, xReplicasOnDifferentMap);
    }

    public List<DeviceLayerKind> getLayerStackList(AccessContext accCtx) throws AccessDeniedException
    {
        return protectedList(accCtx, layerStack);
    }


    public List<DeviceProviderKind> getProviderList(AccessContext accCtx) throws AccessDeniedException
    {
        return protectedList(accCtx, allowedProviderList);
    }

    public Boolean getDisklessOnRemaining(AccessContext accCtxRef) throws AccessDeniedException
    {
        return disklessOnRemaining.get();
    }

    public AutoSelectFilterApi getApiData()
    {
        return new AutoSelectFilterBuilder()
            .setPlaceCount(replicaCount.get())// no "additional" placeCounts for rscGrps
            // copy the lists to avoid showing "TransactionList[...]" in the client. Otherwise we could simply use
            // Collections.unmodifiableList(...)
            .setNodeNameList(new ArrayList<>(nodeNameList))
            .setStorPoolNameList(new ArrayList<>(storPoolNameList))
            .setStorPoolDisklessNameList(new ArrayList<>(storPoolDisklessNameList))
            .setDoNotPlaceWithRscList(new ArrayList<>(doNotPlaceWithRscList))
            .setDoNotPlaceWithRegex(doNotPlaceWithRscRegex.get())
            .setReplicasOnSameList(new ArrayList<>(replicasOnSameList))
            .setReplicasOnDifferentList(new ArrayList<>(replicasOnDifferentList))
            .setXReplicasOnDifferentMap(new HashMap<>(xReplicasOnDifferentMap))
            .setLayerStackList(new ArrayList<>(layerStack))
            .setDeviceProviderKinds(new ArrayList<>(allowedProviderList))
            .setDisklessOnRemaining(disklessOnRemaining.get())
            .build();
    }

    public void applyChanges(AutoSelectFilterApi autoPlaceConfigRef) throws DatabaseException
    {
        if (autoPlaceConfigRef != null)
        {
            apply(autoPlaceConfigRef.getReplicaCount(), replicaCount);
            apply(autoPlaceConfigRef.getNodeNameList(), nodeNameList);
            apply(autoPlaceConfigRef.getStorPoolNameList(), storPoolNameList);
            apply(autoPlaceConfigRef.getStorPoolDisklessNameList(), storPoolDisklessNameList);
            apply(autoPlaceConfigRef.getDisklessOnRemaining(), disklessOnRemaining);
            apply(autoPlaceConfigRef.getDoNotPlaceWithRscList(), doNotPlaceWithRscList);
            apply(autoPlaceConfigRef.getDoNotPlaceWithRscRegex(), doNotPlaceWithRscRegex);
            apply(autoPlaceConfigRef.getLayerStackList(), layerStack);
            apply(autoPlaceConfigRef.getProviderList(), allowedProviderList);
            apply(autoPlaceConfigRef.getReplicasOnSameList(), replicasOnSameList);
            apply(autoPlaceConfigRef.getReplicasOnDifferentList(), replicasOnDifferentList);
            apply(autoPlaceConfigRef.getXReplicasOnDifferentMap(), xReplicasOnDifferentMap);
        }
    }

    private <T> void apply(@Nullable T from, TransactionSimpleObject<?, T> to) throws DatabaseException
    {
        if (from != null)
        {
            to.set(from);
        }
    }

    private <T> void apply(@Nullable List<T> from, List<T> to)
    {
        if (from != null && !from.equals(to))
        {
            to.clear();
            to.addAll(from);
        }
    }

    private <K, V> void apply(@Nullable Map<K, V> from, Map<K, V> to)
    {
        if (from != null && !from.equals(to))
        {
            to.clear();
            to.putAll(from);
        }
    }

    private <T> List<T> protectedList(
        AccessContext accCtx,
        List<T> list
    )
        throws AccessDeniedException
    {
        ObjectProtection objProt = getObjProt();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        List<T> ret;
        AccessType queryAccess = objProt.queryAccess(accCtx);

        if (queryAccess.hasAccess(AccessType.CHANGE))
        {
            ret = list;
        }
        else
        {
            ret = Collections.unmodifiableList(list);
        }
        return ret;
    }

    private <K, V> Map<K, V> protectedMap(AccessContext accCtx, Map<K, V> map)
        throws AccessDeniedException
    {
        ObjectProtection objProt = getObjProt();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        Map<K, V> ret;
        AccessType queryAccess = objProt.queryAccess(accCtx);

        if (queryAccess.hasAccess(AccessType.CHANGE))
        {
            ret = map;
        }
        else
        {
            ret = Collections.unmodifiableMap(map);
        }
        return ret;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
            allowedProviderList,
            disklessOnRemaining,
            doNotPlaceWithRscList,
            doNotPlaceWithRscRegex,
            layerStack,
            nodeNameList,
            replicaCount,
            replicasOnDifferentList,
            replicasOnSameList,
            rscGrp,
            storPoolDisklessNameList,
            storPoolNameList
        );
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof AutoSelectorConfig))
        {
            return false;
        }
        AutoSelectorConfig other = (AutoSelectorConfig) obj;
        return Objects.equals(allowedProviderList, other.allowedProviderList) && Objects.equals(
            disklessOnRemaining,
            other.disklessOnRemaining
        ) && Objects.equals(doNotPlaceWithRscList, other.doNotPlaceWithRscList) && Objects.equals(
            doNotPlaceWithRscRegex,
            other.doNotPlaceWithRscRegex
        ) && Objects.equals(layerStack, other.layerStack) && Objects.equals(nodeNameList, other.nodeNameList) && Objects
            .equals(replicaCount, other.replicaCount) && Objects.equals(
                replicasOnDifferentList,
                other.replicasOnDifferentList
            ) && Objects.equals(replicasOnSameList, other.replicasOnSameList) && Objects.equals(rscGrp, other.rscGrp) &&
            Objects.equals(storPoolDisklessNameList, other.storPoolDisklessNameList) && Objects.equals(
                storPoolNameList,
                other.storPoolNameList
            );
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("AutoSelectorConfig:\n");
        appendIfExists(sb, "replicaCount", replicaCount);
        appendIfNotEmpty(sb, "nodeNameList", nodeNameList);
        appendIfNotEmpty(sb, "storPoolNameList", storPoolNameList);
        appendIfNotEmpty(sb, "storPoolDisklessNameList", storPoolDisklessNameList);
        appendIfNotEmpty(sb, "doNotPlaceWithRscList", doNotPlaceWithRscList);
        appendIfExists(sb, "doNotPlaceWithRscRegex", doNotPlaceWithRscRegex);
        appendIfNotEmpty(sb, "replicasOnSameList", replicasOnSameList);
        appendIfNotEmpty(sb, "replicasOnDifferentList", replicasOnDifferentList);
        appendIfNotEmpty(sb, "xReplicasOnDifferentMap", xReplicasOnDifferentMap);
        appendIfNotEmpty(sb, "layerStack", layerStack);
        appendIfNotEmpty(sb, "allowedProviderList", allowedProviderList);
        appendIfExists(sb, "disklessOnRemaining", disklessOnRemaining);
        return sb.toString();
    }


    private void appendIfExists(StringBuilder sb, String description, TransactionSimpleObject<?, ?> txSimpleObj)
    {
        @Nullable Object obj = txSimpleObj.get();
        if (obj != null)
        {
            sb.append("\t").append(description).append(": ").append(obj).append("\n");
        }
    }

    private void appendIfNotEmpty(StringBuilder sb, String description, TransactionList<?, ?> txList)
    {
        if (!txList.isEmpty())
        {
            sb.append("\t").append(description).append(": ").append(txList).append("\n");
        }
    }

    private void appendIfNotEmpty(StringBuilder sb, String description, TransactionMap<?, ?, ?> txLMap)
    {
        if (!txLMap.isEmpty())
        {
            sb.append("\t").append(description).append(": ").append(txLMap).append("\n");
        }
    }
}
