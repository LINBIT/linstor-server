package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
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
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

public class ResourceGroup extends AbsCoreObj<ResourceGroup>
{
    public interface InitMaps
    {
        Map<VolumeNumber, VolumeGroup> getVlmGrpMap();
        Map<ResourceName, ResourceDefinition> getRscDfnMap();
    }


    private final ResourceGroupName name;

    private final TransactionSimpleObject<ResourceGroup, @Nullable String> description;

    private final Props rscGrpProps;

    private final TransactionMap<ResourceGroup, VolumeNumber, VolumeGroup> vlmMap;

    private final AutoSelectorConfig autoPlaceConfig;

    private final TransactionMap<ResourceGroup, ResourceName, ResourceDefinition> rscDfnMap;
    private final TransactionSimpleObject<ResourceGroup, Short> peerSlots;

    private final ResourceGroupDatabaseDriver dbDriver;

    public ResourceGroup(
        UUID uuidRef,
        ResourceGroupName rscGrpNameRef,
        @Nullable String descriptionRef,
        @Nullable List<DeviceLayerKind> autoPlaceLayerStackRef,
        @Nullable Integer autoPlaceReplicaCountRef,
        @Nullable List<String> autoPlaceNodeNameListRef,
        @Nullable List<String> autoPlaceStorPoolNameListRef,
        @Nullable List<String> autoPlaceStorPoolDisklessNameListRef,
        @Nullable List<String> autoPlaceDoNotPlaceWithRscListRef,
        @Nullable String autoPlaceDoNotPlaceWithRscRegexRef,
        @Nullable List<String> autoPlaceReplicasOnSameListRef,
        @Nullable List<String> autoPlaceReplicasOnDifferentListRef,
        @Nullable Map<String, Integer> autoPlaceXReplicasOnDifferentMapRef,
        @Nullable List<DeviceProviderKind> autoPlaceAllowedProviderListRef,
        @Nullable Boolean autoPlaceDisklessOnRemainingRef,
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

        description = transObjFactory.createTransactionSimpleObject(
            this,
            descriptionRef,
            dbDriverRef.getDescriptionDriver()
        );

        rscGrpProps = propsContainerFactoryRef.getInstance(
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
            rscGrpProps,
            autoPlaceConfig,
            vlmMap,
            peerSlots,
            deleted
        );
    }

    public ResourceGroupName getName()
    {
        checkDeleted();
        return name;
    }

    public @Nullable String getDescription()
    {
        checkDeleted();
        return description.get();
    }

    public @Nullable String setDescription(@Nullable String descriptionRef)
        throws DatabaseException
    {
        checkDeleted();
        return description.set(descriptionRef);
    }

    public void addResourceDefinition(ResourceDefinition rscDfnRef)
    {
        checkDeleted();
        rscDfnMap.put(rscDfnRef.getName(), rscDfnRef);
    }

    public void removeResourceDefinition(ResourceDefinition rscDfnRef)
    {
        checkDeleted();
        rscDfnMap.remove(rscDfnRef.getName());
    }

    public boolean hasResourceDefinitions()
    {
        checkDeleted();
        return !rscDfnMap.isEmpty();
    }

    public Props getProps()
    {
        checkDeleted();
        return rscGrpProps;
    }

    /**
     * Returns the {@link Props} from {@link #getVolumeGroup(VolumeNumber)} but instead of
     * a possible {@link NullPointerException} (in case the {@link VolumeGroup} does not exist) this method
     * returns an empty {@link ReadOnlyPropsImpl} instance.
     *
     */
    public ReadOnlyProps getVolumeGroupProps(VolumeNumber vlmNrRef)
    {
        checkDeleted();

        VolumeGroup vlmGrp = vlmMap.get(vlmNrRef);

        ReadOnlyProps vlmGrpProps;
        if (vlmGrp == null)
        {
            vlmGrpProps = ReadOnlyPropsImpl.emptyRoProps();
        }
        else
        {
            vlmGrpProps = vlmGrp.getProps();
        }
        return vlmGrpProps;
    }

    public AutoSelectorConfig getAutoPlaceConfig()
    {
        checkDeleted();
        return autoPlaceConfig;
    }

    public Stream<VolumeGroup> streamVolumeGroups()
    {
        checkDeleted();
        return vlmMap.values().stream();
    }

    public @Nullable VolumeGroup getVolumeGroup(VolumeNumber vlmNr)
    {
        checkDeleted();
        return vlmMap.get(vlmNr);
    }

    public List<VolumeGroup> getVolumeGroups()
    {
        checkDeleted();
        return Collections.unmodifiableList(new ArrayList<>(vlmMap.values()));
    }

    public void putVolumeGroup(VolumeGroup vlmGrpDataRef)
    {
        checkDeleted();
        vlmMap.put(vlmGrpDataRef.getVolumeNumber(), vlmGrpDataRef);
    }

    public void deleteVolumeGroup(VolumeNumber vlmNrRef)
    {
        checkDeleted();
        vlmMap.remove(vlmNrRef);
    }

    public Collection<ResourceDefinition> getRscDfns()
    {
        checkDeleted();
        return Collections.unmodifiableCollection(rscDfnMap.values());
    }

    public @Nullable Short getPeerSlots()
    {
        checkDeleted();
        return peerSlots.get();
    }

    public void setPeerSlots(@Nullable Short peerSlotsRef)
        throws DatabaseException
    {
        checkDeleted();
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
        else if (obj instanceof ResourceGroup other)
        {
            other.checkDeleted();
            ret = Objects.equals(name, other.name);
        }
        return ret;
    }

    public ResourceGroupApi getApiData()
    {
        checkDeleted();
        List<VolumeGroupApi> vlmGrpApiList = new ArrayList<>(vlmMap.size());
        for (VolumeGroup vlmGrp : vlmMap.values())
        {
            vlmGrpApiList.add(vlmGrp.getApiData());
        }
        return new RscGrpPojo(
            objId,
            name.displayValue,
            description.get(),
            rscGrpProps.cloneMap(),
            vlmGrpApiList,
            autoPlaceConfig.getApiData(),
            peerSlots.get()
        );
    }

    @Override
    public void delete() throws DatabaseException
    {
        if (!deleted.get())
        {

            if (!rscDfnMap.isEmpty())
            {
                throw new ImplementationError("Resouce group with existing resource definitions cannot be deleted");
            }

            // Shallow copy the collection because elements may be removed from it
            List<VolumeGroup> tmpMap = new ArrayList<>(vlmMap.values());
            for (VolumeGroup vlmGrp : tmpMap)
            {
                vlmGrp.delete();
            }

            rscGrpProps.delete();

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
