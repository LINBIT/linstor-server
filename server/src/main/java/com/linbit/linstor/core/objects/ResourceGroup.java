package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apis.ResourceGroupApi;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

public class ResourceGroup extends AbsCoreObj<ResourceGroup> implements ProtectedObject
{
    public interface InitMaps
    {
        Map<VolumeNumber, VolumeGroup> getVlmGrpMap();
        Map<ResourceName, ResourceDefinition> getRscDfnMap();
    }

    private final ObjectProtection objProt;

    private final ResourceGroupName name;

    private final TransactionSimpleObject<ResourceGroup, String> description;

    private final Props rscDfnGrpProps;

    private final TransactionMap<ResourceGroup, VolumeNumber, VolumeGroup> vlmMap;

    private final AutoSelectorConfig autoPlaceConfig;

    private final TransactionMap<ResourceGroup, ResourceName, ResourceDefinition> rscDfnMap;
    private final TransactionSimpleObject<ResourceGroup, Short> peerSlots;

    private final ResourceGroupDatabaseDriver dbDriver;

    public ResourceGroup(
        UUID uuidRef,
        ObjectProtection objProtRef,
        ResourceGroupName rscGrpNameRef,
        String descriptionRef,
        List<DeviceLayerKind> autoPlaceLayerStackRef,
        Integer autoPlaceReplicaCountRef,
        List<String> autoPlaceNodeNameListRef,
        List<String> autoPlaceStorPoolNameListRef,
        List<String> autoPlaceStorPoolDisklessNameListRef,
        List<String> autoPlaceDoNotPlaceWithRscListRef,
        String autoPlaceDoNotPlaceWithRscRegexRef,
        List<String> autoPlaceReplicasOnSameListRef,
        List<String> autoPlaceReplicasOnDifferentListRef,
        Map<String, Integer> autoPlaceXReplicasOnDifferentMapRef,
        List<DeviceProviderKind> autoPlaceAllowedProviderListRef,
        Boolean autoPlaceDisklessOnRemainingRef,
        Map<VolumeNumber, VolumeGroup> vlmGrpMapRef,
        Map<ResourceName, ResourceDefinition> rscDfnMapRef,
        @Nullable Short peerSlotsRef,
        ResourceGroupDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
        throws DatabaseException
    {
        super(uuidRef, transObjFactory, transMgrProvider);
        name = rscGrpNameRef;
        dbDriver = dbDriverRef;
        objProt = objProtRef;

        description = transObjFactory.createTransactionSimpleObject(
            this,
            descriptionRef,
            dbDriverRef.getDescriptionDriver()
        );

        rscDfnGrpProps = propsContainerFactoryRef.getInstance(
            PropsContainer.buildPath(rscGrpNameRef),
            toStringImpl(),
            LinStorObject.RSC_GRP
        );
        vlmMap = transObjFactory.createTransactionMap(this, vlmGrpMapRef, null);

        rscDfnMap = transObjFactory.createTransactionMap(this, rscDfnMapRef, null);

        autoPlaceConfig = new AutoSelectorConfig(
            this,
            autoPlaceReplicaCountRef,
            autoPlaceNodeNameListRef,
            autoPlaceStorPoolNameListRef,
            autoPlaceStorPoolDisklessNameListRef,
            autoPlaceDoNotPlaceWithRscListRef,
            autoPlaceDoNotPlaceWithRscRegexRef,
            autoPlaceReplicasOnSameListRef,
            autoPlaceReplicasOnDifferentListRef,
            autoPlaceXReplicasOnDifferentMapRef,
            autoPlaceLayerStackRef,
            autoPlaceAllowedProviderListRef,
            autoPlaceDisklessOnRemainingRef,
            dbDriverRef,
            transObjFactory,
            transMgrProvider
        );

        peerSlots = transObjFactory.createTransactionSimpleObject(this, peerSlotsRef, dbDriver.getPeerSlotsDriver());

        transObjs = Arrays.asList(
            objProt,
            rscDfnGrpProps,
            autoPlaceConfig,
            vlmMap,
            peerSlots,
            deleted
        );
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    public ResourceGroupName getName()
    {
        checkDeleted();
        return name;
    }

    public String getDescription(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return description.get();
    }

    public String setDescription(AccessContext accCtx, String descriptionRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        return description.set(descriptionRef);
    }

    public void addResourceDefinition(AccessContext accCtx, ResourceDefinition rscDfnRef)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        rscDfnMap.put(rscDfnRef.getName(), rscDfnRef);
    }

    public void removeResourceDefinition(AccessContext accCtx, ResourceDefinition rscDfnRef)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        rscDfnMap.remove(rscDfnRef.getName());
    }

    public boolean hasResourceDefinitions(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return !rscDfnMap.isEmpty();
    }

    public Props getProps(AccessContext accCtxRef)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtxRef, objProt, rscDfnGrpProps);
    }

    /**
     * Returns the {@link Props} from {@link #getVolumeGroup(AccessContext, VolumeNumber)} but instead of
     * a possible {@link NullPointerException} (in case the {@link VolumeGroup} does not exist) this method
     * returns an empty {@link ReadOnlyPropsImpl} instance.
     *
     * @param accCtx
     * @param vlmNrRef
     * @throws AccessDeniedException
     */
    public ReadOnlyProps getVolumeGroupProps(AccessContext accCtx, VolumeNumber vlmNrRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        VolumeGroup vlmGrp = vlmMap.get(vlmNrRef);

        ReadOnlyProps vlmGrpProps;
        if (vlmGrp == null)
        {
            vlmGrpProps = ReadOnlyPropsImpl.emptyRoProps();
        }
        else
        {
            vlmGrpProps = vlmGrp.getProps(accCtx);
        }
        return vlmGrpProps;
    }

    public AutoSelectorConfig getAutoPlaceConfig()
    {
        checkDeleted();
        return autoPlaceConfig;
    }

    public Stream<VolumeGroup> streamVolumeGroups(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return vlmMap.values().stream();
    }

    public VolumeGroup getVolumeGroup(AccessContext accCtx, VolumeNumber vlmNr)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return vlmMap.get(vlmNr);
    }

    public List<VolumeGroup> getVolumeGroups(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return Collections.unmodifiableList(new ArrayList<>(vlmMap.values()));
    }

    public void putVolumeGroup(AccessContext accCtx, VolumeGroup vlmGrpDataRef)
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

    public Collection<ResourceDefinition> getRscDfns(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return Collections.unmodifiableCollection(rscDfnMap.values());
    }

    public @Nullable Short getPeerSlots(@Nonnull AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return peerSlots.get();
    }

    public void setPeerSlots(@Nonnull AccessContext accCtx, @Nullable Short peerSlotsRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        peerSlots.set(peerSlotsRef);
    }

    @Override
    public int compareTo(ResourceGroup other)
    {
        checkDeleted();
        return name.compareTo(other.getName());
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof ResourceGroup)
        {
            ResourceGroup other = (ResourceGroup) obj;
            other.checkDeleted();
            ret = Objects.equals(name, other.name);
        }
        return ret;
    }

    public ResourceGroupApi getApiData(AccessContext accCtxRef) throws AccessDeniedException
    {
        checkDeleted();
        List<VolumeGroupApi> vlmGrpApiList = new ArrayList<>(vlmMap.size());
        for (VolumeGroup vlmGrp : vlmMap.values())
        {
            vlmGrpApiList.add(vlmGrp.getApiData(accCtxRef));
        }
        return new RscGrpPojo(
            objId,
            name.displayValue,
            description.get(),
            rscDfnGrpProps.map(),
            vlmGrpApiList,
            autoPlaceConfig.getApiData(),
            peerSlots.get()
        );
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

            deleted.set(Boolean.TRUE);
        }
    }

    @Override
    protected String toStringImpl()
    {
        return "ResourceGroup '" + name + "'";
    }
}
