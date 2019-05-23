package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.VolumeGroup.VlmGrpApi;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

public class ResourceGroupData extends BaseTransactionObject implements ResourceGroup
{
    private final UUID objId;

    private final transient UUID dbgInstanceId;

    private final ObjectProtection objProt;

    private final ResourceGroupName name;

    private final TransactionSimpleObject<ResourceGroupData, String> description;

    private final TransactionList<ResourceGroupData, DeviceLayerKind> layerStack;

    private final Props rscDfnGrpProps;

    private final TransactionMap<VolumeNumber, VolumeGroup> vlmMap;

    private final AutoSelectorConfigData autoPlaceConfig;

    private final TransactionMap<ResourceName, ResourceDefinition> rscDfnMap;

    private final ResourceGroupDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<ResourceGroupData, Boolean> deleted;

    ResourceGroupData(
        UUID uuidRef,
        ObjectProtection objProtRef,
        ResourceGroupName rscGrpNameRef,
        String descriptionRef,
        List<DeviceLayerKind> layerStackRef,
        Integer autoPlaceReplicaCountRef,
        String autoPlaceStorPoolNameRef,
        List<String> autoPlaceDoNotPlaceWithRscListRef,
        String autoPlaceDoNotPlaceWithRscRegexRef,
        List<String> autoPlaceReplicasOnSameListRef,
        List<String> autoPlaceReplicasOnDifferentListRef,
        List<DeviceProviderKind> autoPlaceAllowedProviderListRef,
        Map<VolumeNumber, VolumeGroup> vlmGrpMapRef,
        Map<ResourceName, ResourceDefinition> rscDfnMapRef,
        ResourceGroupDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        objId = uuidRef;
        dbgInstanceId = UUID.randomUUID();
        name = rscGrpNameRef;
        dbDriver = dbDriverRef;
        objProt = objProtRef;

        description = transObjFactory.createTransactionSimpleObject(
            this,
            descriptionRef,
            dbDriverRef.getDescriptionDriver()
        );

        rscDfnGrpProps = propsContainerFactoryRef.create(
            PropsContainer.buildPath(rscGrpNameRef)
        );
        vlmMap = transObjFactory.createTransactionMap(vlmGrpMapRef, null);
        layerStack = transObjFactory.createTransactionPrimitiveList(
            this,
            layerStackRef,
            dbDriverRef.getLayerStackDriver()
        );

        rscDfnMap = transObjFactory.createTransactionMap(rscDfnMapRef, null);

        autoPlaceConfig = new AutoSelectorConfigData(
            this,
            autoPlaceReplicaCountRef,
            autoPlaceStorPoolNameRef,
            autoPlaceDoNotPlaceWithRscListRef,
            autoPlaceDoNotPlaceWithRscRegexRef,
            autoPlaceReplicasOnSameListRef,
            autoPlaceReplicasOnDifferentListRef,
            layerStack,
            autoPlaceAllowedProviderListRef,
            dbDriverRef,
            transObjFactory,
            transMgrProvider
        );

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            objProt,
            rscDfnGrpProps,
            layerStack,
            autoPlaceConfig,
            vlmMap,
            deleted
        );
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return objProt;
    }

    @Override
    public ResourceGroupName getName()
    {
        checkDeleted();
        return name;
    }

    @Override
    public String getDescription(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return description.get();
    }

    @Override
    public String setDescription(AccessContext accCtx, String descriptionRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        return description.set(descriptionRef);
    }

    @Override
    public boolean hasResourceDefinitions(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return !rscDfnMap.isEmpty();
    }


    @Override
    public Props getRscDfnGrpProps(AccessContext accCtxRef)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtxRef, objProt, rscDfnGrpProps);
    }

    @Override
    public List<DeviceLayerKind> getLayerStack(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return layerStack;
    }

    public void setLayerStack(AccessContext accCtx, List<DeviceLayerKind> layerStackRef)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        layerStack.clear();
        layerStack.addAll(layerStackRef);
    }

    @Override
    public AutoSelectorConfig getAutoPlaceConfig()
    {
        checkDeleted();
        return autoPlaceConfig;
    }

    @Override
    public Stream<VolumeGroup> streamVolumeGroups(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return vlmMap.values().stream();
    }

    @Override
    public VolumeGroup getVolumeGroup(AccessContext accCtx, VolumeNumber vlmNr)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return vlmMap.get(vlmNr);
    }

    public void putVolumeGroup(AccessContext accCtx, VolumeGroupData vlmGrpDataRef)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        vlmMap.put(vlmGrpDataRef.getVolumeNumber(), vlmGrpDataRef);
    }

    public void deleteVolumeGroup(AccessContext accCtx, VolumeNumber vlmNrRef)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        vlmMap.remove(vlmNrRef);
    }

    @Override
    public Collection<ResourceDefinition> getRscDfns(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return Collections.unmodifiableCollection(rscDfnMap.values());
    }


    @Override
    public int compareTo(ResourceGroup other)
    {
        checkDeleted();
        return name.compareTo(other.getName());
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            if (!rscDfnMap.isEmpty())
            {
                throw new ImplementationError("Resouce group with existing resource definitions cannot be deleted");
            }

            // Shallow copy the collection because elements may be removed from it
            List<VolumeGroup> tmpMap = new ArrayList<>(vlmMap.values());
            for (VolumeGroup vlmGrp : tmpMap)
            {
                vlmGrp.delete(accCtx);
            }

            rscDfnGrpProps.delete();

            objProt.delete(accCtx);
            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted resource group");
        }
    }

    @Override
    public RscGrpApi getApiData(AccessContext accessContextRef) throws AccessDeniedException
    {
        List<VlmGrpApi> vlmGrpApiList = new ArrayList<>();
        for (VolumeGroup vlmGrp : vlmMap.values())
        {
            vlmGrpApiList.add(vlmGrp.getApiData(accessContextRef));
        }
        return new RscGrpPojo(
            objId,
            name.displayValue,
            description.get(),
            new ArrayList<>(layerStack),
            new TreeMap<>(rscDfnGrpProps.map()),
            vlmGrpApiList,
            autoPlaceConfig == null ? null : autoPlaceConfig.getApiData(accessContextRef)
        );
    }
}
