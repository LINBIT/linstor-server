package com.linbit.linstor.test.factories;

import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.ResourceGroupControllerFactory;
import com.linbit.linstor.core.objects.ResourceGroupDbDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.TestAccessContextProvider;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import static com.linbit.linstor.test.factories.TestFactoryUtils.copyOrNull;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Singleton
public class ResourceGroupTestFactory
{
    private final ResourceGroupControllerFactory rscGrpFact;
    private final ResourceGroupDatabaseDriver rscGrpDbDriver; // needed to load existing "dfltRscGrp"

    private final HashMap<String, ResourceGroup> rscGrpMap = new HashMap<>();

    private String dfltRscGrpNamePattern = "rscGrp-%02d";
    private AtomicInteger nextId = new AtomicInteger(1);

    private AccessContext dfltAccCtx = TestAccessContextProvider.PUBLIC_CTX;
    private Supplier<String> dfltRscGrpNameSupplier = () -> String.format(
        dfltRscGrpNamePattern,
        nextId.incrementAndGet()
    );
    private String dfltDescription = null;
    private List<DeviceLayerKind> dfltLayerStack = null;
    private Integer dfltAutoPlaceReplicaCount = null;
    private List<String> dfltAutoPlaceNodeNameList = null;
    private List<String> dfltAutoPlaceStorPoolList = null;
    private List<String> dfltAutoPlaceStorPoolDisklessList = null;
    private List<String> dfltAutoPlaceDoNotPlaceWithRscList = null;
    private String dfltAutoPlaceDoNotPlaceWithRscRegex = null;
    private List<String> dfltAutoPlaceReplicasOnSameList = null;
    private List<String> dfltAutoPlaceReplicasOnDifferentList = null;
    private Map<String, Integer> dfltAutoPlaceXReplicasOnDifferentMap = null;
    private List<DeviceProviderKind> dfltAutoPlaceAllowedProviderList = null;
    private Boolean dfltAutoPlaceDisklessOnRemaining = null;
    private @Nullable Short dfltPeerSlots = null;

    @Inject
    public ResourceGroupTestFactory(
        ResourceGroupControllerFactory rscGrpFactRef,
        ResourceGroupDatabaseDriver rscGrpDbDriverRef
    )
        throws DatabaseException, AccessDeniedException
    {
        rscGrpFact = rscGrpFactRef;
        rscGrpDbDriver = rscGrpDbDriverRef;
    }

    /*
     * This method has to be called as a workaround. We cannot create "dfltRscGrp" as it already exists in DB
     * But we cannot load it in the constructor (during guice injection) as we are not in a scope at the at time.
     */
    public void initDfltRscGrp() throws AccessDeniedException, DatabaseException
    {
        ResourceGroup rscGrp = ((ResourceGroupDbDriver) rscGrpDbDriver).loadAll(null).keySet().stream()
            .filter(grp -> grp.getName().displayValue.equals(InternalApiConsts.DEFAULT_RSC_GRP_NAME))
            .findFirst()
            .get();
        rscGrp.getObjProt().addAclEntry(
            TestAccessContextProvider.INIT_CTX,
            TestAccessContextProvider.SYS_CTX.subjectRole,
            AccessType.CONTROL
        );

        rscGrpMap.put(InternalApiConsts.DEFAULT_RSC_GRP_NAME.toUpperCase(), rscGrp);
    }

    public ResourceGroup get(String rscGroupNameRef, boolean createIfNotExists)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        ResourceGroup rscGrp = rscGrpMap.get(rscGroupNameRef.toUpperCase());
        if (rscGrp == null && createIfNotExists)
        {
            rscGrp = create(rscGroupNameRef);
        }
        return rscGrp;
    }

    public ResourceGroupTestFactory setDfltAccCtx(AccessContext dfltAccCtxRef)
    {
        dfltAccCtx = dfltAccCtxRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltRscGrpNamePattern(String dfltRscGrpNamePatternRef)
    {
        dfltRscGrpNamePattern = dfltRscGrpNamePatternRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltRscGrpNameSupplier(Supplier<String> dfltRscGrpNameSupplierRef)
    {
        dfltRscGrpNameSupplier = dfltRscGrpNameSupplierRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltDescription(String dfltDescriptionRef)
    {
        dfltDescription = dfltDescriptionRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltLayerStack(List<DeviceLayerKind> dfltLayerStackRef)
    {
        dfltLayerStack = dfltLayerStackRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltAutoPlaceReplicaCount(Integer dfltAutoPlaceReplicaCountRef)
    {
        dfltAutoPlaceReplicaCount = dfltAutoPlaceReplicaCountRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltAutoPlaceNodeNameList(List<String> dfltAutoPlaceNodeNameListRef)
    {
        dfltAutoPlaceNodeNameList = dfltAutoPlaceNodeNameListRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltAutoPlaceStorPoolList(List<String> dfltAutoPlaceStorPoolListRef)
    {
        dfltAutoPlaceStorPoolList = dfltAutoPlaceStorPoolListRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltAutoPlaceStorPoolDisklessList(
        List<String> dfltAutoPlaceStorPoolDisklessListRef
    )
    {
        dfltAutoPlaceStorPoolDisklessList = dfltAutoPlaceStorPoolDisklessListRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltAutoPlaceDoNotPlaceWithRscList(
        List<String> dfltAutoPlaceDoNotPlaceWithRscListRef
    )
    {
        dfltAutoPlaceDoNotPlaceWithRscList = dfltAutoPlaceDoNotPlaceWithRscListRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltAutoPlaceDoNotPlaceWithRscRegex(
        String dfltAutoPlaceDoNotPlaceWithRscRegexRef
    )
    {
        dfltAutoPlaceDoNotPlaceWithRscRegex = dfltAutoPlaceDoNotPlaceWithRscRegexRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltAutoPlaceReplicasOnSameList(List<String> dfltAutoPlaceReplicasOnSameListRef)
    {
        dfltAutoPlaceReplicasOnSameList = dfltAutoPlaceReplicasOnSameListRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltAutoPlaceReplicasOnDifferentList(
        List<String> dfltAutoPlaceReplicasOnDifferentListRef
    )
    {
        dfltAutoPlaceReplicasOnDifferentList = dfltAutoPlaceReplicasOnDifferentListRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltAutoPlaceXReplicasOnDifferentMap(
        Map<String, Integer> dfltAutoPlaceXReplicasOnDifferentMapRef
    )
    {
        dfltAutoPlaceXReplicasOnDifferentMap = dfltAutoPlaceXReplicasOnDifferentMapRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltAutoPlaceAllowedProviderList(
        List<DeviceProviderKind> dfltAutoPlaceAllowedProviderListRef
    )
    {
        dfltAutoPlaceAllowedProviderList = dfltAutoPlaceAllowedProviderListRef;
        return this;
    }

    public ResourceGroupTestFactory setDfltAutoPlaceDisklessOnRemaining(Boolean dfltAutoPlaceDisklessOnRemainingRef)
    {
        dfltAutoPlaceDisklessOnRemaining = dfltAutoPlaceDisklessOnRemainingRef;
        return this;
    }

    public ResourceGroup create()
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        return builder().build();
    }

    public ResourceGroup create(String rscGrpName)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        return builder(rscGrpName).build();
    }

    public ResourceGroupBuilder builder()
    {
        return new ResourceGroupBuilder(dfltRscGrpNameSupplier.get());
    }

    public ResourceGroupBuilder builder(String rscGrpName)
    {
        return new ResourceGroupBuilder(rscGrpName);
    }

    public class ResourceGroupBuilder
    {
        private String rscGrpName;
        private AccessContext accCtx;
        private String description;
        private List<DeviceLayerKind> layerStack;
        private Integer autoPlaceReplicaCount;
        private List<String> autoPlaceNodeNameList;
        private List<String> autoPlaceStorPoolList;
        private List<String> autoPlaceStorPoolDisklessList;
        private List<String> autoPlaceDoNotPlaceWithRscList;
        private String autoPlaceDoNotPlaceWithRscRegex;
        private List<String> autoPlaceReplicasOnSameList;
        private List<String> autoPlaceReplicasOnDifferentList;
        private Map<String, Integer> autoPlaceXReplicasOnDifferentMap;
        private List<DeviceProviderKind> autoPlaceAllowedProviderList;
        private Boolean autoPlaceDisklessOnRemaining;
        private @Nullable Short peerSlots;

        public ResourceGroupBuilder(String rscGrpNameRef)
        {
            rscGrpName = rscGrpNameRef;

            accCtx = dfltAccCtx;
            description = dfltDescription;
            layerStack = copyOrNull(dfltLayerStack);
            autoPlaceReplicaCount = dfltAutoPlaceReplicaCount;
            autoPlaceNodeNameList = copyOrNull(dfltAutoPlaceNodeNameList);
            autoPlaceStorPoolList = copyOrNull(dfltAutoPlaceStorPoolList);
            autoPlaceStorPoolDisklessList = copyOrNull(dfltAutoPlaceStorPoolDisklessList);
            autoPlaceDoNotPlaceWithRscList = copyOrNull(dfltAutoPlaceDoNotPlaceWithRscList);
            autoPlaceDoNotPlaceWithRscRegex = dfltAutoPlaceDoNotPlaceWithRscRegex;
            autoPlaceReplicasOnSameList = copyOrNull(dfltAutoPlaceReplicasOnSameList);
            autoPlaceReplicasOnDifferentList = copyOrNull(dfltAutoPlaceReplicasOnDifferentList);
            autoPlaceXReplicasOnDifferentMap = copyOrNull(dfltAutoPlaceXReplicasOnDifferentMap);
            autoPlaceAllowedProviderList = copyOrNull(dfltAutoPlaceAllowedProviderList);
            autoPlaceDisklessOnRemaining = dfltAutoPlaceDisklessOnRemaining;
            peerSlots = dfltPeerSlots;
        }

        public ResourceGroupBuilder setRscGrpName(String rscGrpNameRef)
        {
            rscGrpName = rscGrpNameRef;
            return this;
        }

        public ResourceGroupBuilder setAccCtx(AccessContext accCtxRef)
        {
            accCtx = accCtxRef;
            return this;
        }

        public ResourceGroupBuilder setDescription(String descriptionRef)
        {
            description = descriptionRef;
            return this;
        }

        public ResourceGroupBuilder setLayerStack(List<DeviceLayerKind> layerStackRef)
        {
            layerStack = layerStackRef;
            return this;
        }

        public ResourceGroupBuilder setAutoPlaceReplicaCount(Integer autoPlaceReplicaCountRef)
        {
            autoPlaceReplicaCount = autoPlaceReplicaCountRef;
            return this;
        }

        public ResourceGroupBuilder setAutoPlaceNodeNameList(List<String> autoPlaceNodeNameListRef)
        {
            autoPlaceNodeNameList = autoPlaceNodeNameListRef;
            return this;
        }

        public ResourceGroupBuilder setAutoPlaceStorPoolList(List<String> autoPlaceStorPoolListRef)
        {
            autoPlaceStorPoolList = autoPlaceStorPoolListRef;
            return this;
        }

        public ResourceGroupBuilder setAutoPlaceStorPoolDisklessList(List<String> autoPlaceStorPoolDisklessListRef)
        {
            autoPlaceStorPoolDisklessList = autoPlaceStorPoolDisklessListRef;
            return this;
        }

        public ResourceGroupBuilder setAutoPlaceDoNotPlaceWithRscList(List<String> autoPlaceDoNotPlaceWithRscListRef)
        {
            autoPlaceDoNotPlaceWithRscList = autoPlaceDoNotPlaceWithRscListRef;
            return this;
        }

        public ResourceGroupBuilder setAutoPlaceDoNotPlaceWithRscRegex(String autoPlaceDoNotPlaceWithRscRegexRef)
        {
            autoPlaceDoNotPlaceWithRscRegex = autoPlaceDoNotPlaceWithRscRegexRef;
            return this;
        }

        public ResourceGroupBuilder setAutoPlaceReplicasOnSameList(List<String> autoPlaceReplicasOnSameListRef)
        {
            autoPlaceReplicasOnSameList = autoPlaceReplicasOnSameListRef;
            return this;
        }

        public ResourceGroupBuilder setAutoPlaceReplicasOnDifferentList(
            List<String> autoPlaceReplicasOnDifferentListRef
        )
        {
            autoPlaceReplicasOnDifferentList = autoPlaceReplicasOnDifferentListRef;
            return this;
        }

        public ResourceGroupBuilder setAutoPlaceAllowedProviderList(
            List<DeviceProviderKind> autoPlaceAllowedProviderListRef
        )
        {
            autoPlaceAllowedProviderList = autoPlaceAllowedProviderListRef;
            return this;
        }

        public ResourceGroupBuilder setAutoPlaceDisklessOnRemaining(Boolean autoPlaceDisklessOnRemainingRef)
        {
            autoPlaceDisklessOnRemaining = autoPlaceDisklessOnRemainingRef;
            return this;
        }

        public ResourceGroup build()
            throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, InvalidNameException
        {
            ResourceGroup rscGrp = rscGrpFact.create(
                accCtx,
                new ResourceGroupName(rscGrpName),
                description,
                layerStack,
                autoPlaceReplicaCount,
                autoPlaceNodeNameList,
                autoPlaceStorPoolList,
                autoPlaceStorPoolDisklessList,
                autoPlaceDoNotPlaceWithRscList,
                autoPlaceDoNotPlaceWithRscRegex,
                autoPlaceReplicasOnSameList,
                autoPlaceReplicasOnDifferentList,
                autoPlaceXReplicasOnDifferentMap,
                autoPlaceAllowedProviderList,
                autoPlaceDisklessOnRemaining,
                peerSlots
            );
            rscGrpMap.put(rscGrpName.toUpperCase(), rscGrp);
            return rscGrp;
        }
    }
}
