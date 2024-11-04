package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.SnapshotShippingListItemPojo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.SnapshotDefinitionListItemApi;
import com.linbit.linstor.core.apis.SnapshotShippingListItemApi;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition.Flags;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.ExceptionThrowingPredicate;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.makeSnapshotContext;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

@Deprecated(forRemoval = true)
@Singleton
public class CtrlSnapshotShippingApiCallHandler
{
    private static final String DUMMY_SNAPSHOT_SHIPPING_NAME = "ship$Generated";
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final ResponseConverter responseConverter;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlRscConnectionHelper rscConnHelper;
    private final CtrlPropsHelper propsHelper;

    private final CtrlSnapshotCrtHelper snapCrtHelper;
    private final CtrlSnapshotCrtApiCallHandler snapCrtHandler;
    private final DynamicNumberPool snapshotShippingPortPool;

    @Inject
    public CtrlSnapshotShippingApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlRscConnectionHelper rscConnHelperRef,
        CtrlSnapshotCrtHelper snapCrtHelperRef,
        CtrlSnapshotCrtApiCallHandler snapCrtHandlerRef,
        CtrlPropsHelper propsHelperRef,
        @Named(NumberPoolModule.SNAPSHOPT_SHIPPING_PORT_POOL) DynamicNumberPool snapshotShippingPortPoolRef
    )
    {
        apiCtx = apiCtxRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        responseConverter = responseConverterRef;
        peerAccCtx = peerAccCtxRef;
        lockGuardFactory = lockGuardFactoryRef;
        rscConnHelper = rscConnHelperRef;
        snapCrtHelper = snapCrtHelperRef;
        snapCrtHandler = snapCrtHandlerRef;
        propsHelper = propsHelperRef;
        snapshotShippingPortPool = snapshotShippingPortPoolRef;
    }

    public Flux<ApiCallRc> autoShipSnapshot(String rscNameRef)
    {
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeModifyOperation(),
            Collections.emptyList(),
            rscNameRef,
            ApiConsts.VAL_SNAP_SHIP_NAME
        );

        return scopeRunner.fluxInTransactionalScope(
            "Ship snapshot",
            lockGuardFactory.create() // TODO recheck required locks
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> autoShipSnapshotInTransaction(rscNameRef)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> autoShipSnapshotInTransaction(String rscNameRef)
    {
        Flux<ApiCallRc> flux;
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
        ReadOnlyProps rscDfnProps = propsHelper.getProps(rscDfn);
        String targetNodeName = rscDfnProps.getProp(ApiConsts.KEY_TARGET_NODE, ApiConsts.NAMESPC_SNAPSHOT_SHIPPING);
        String sourceNodeName = rscDfnProps.getProp(ApiConsts.KEY_SOURCE_NODE, ApiConsts.NAMESPC_SNAPSHOT_SHIPPING);

        // TODO: add configurable prefNics

        if (targetNodeName == null)
        {
            flux = Flux.<ApiCallRc>just(
                new ApiCallRcImpl(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_SNAPSHOT_SHIPPING_TARGET,
                        "Property " + ApiConsts.NAMESPC_SNAPSHOT_SHIPPING + "/" + ApiConsts.KEY_TARGET_NODE +
                            " must be set"
                    )
                )
            );
        }
        else
        {
            if (sourceNodeName == null)
            {
                sourceNodeName = getActiveResourceNodeName(rscDfn);
            }
            flux = shipSnapshotInTransaction(rscNameRef, sourceNodeName, targetNodeName, null, true);
        }
        return flux;
    }

    private String getActiveResourceNodeName(ResourceDefinition rscDfnRef)
    {
        String ret = null;
        try
        {
            Iterator<Resource> rscIt = rscDfnRef.iterateResource(peerAccCtx.get());
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                if (rsc.getStateFlags().isUnset(peerAccCtx.get(), Resource.Flags.INACTIVE))
                {
                    ret = rsc.getNode().getName().displayValue;
                    break;
                }
            }
            if (ret == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_NOT_FOUND_RSC,
                        "Failed to find an active resource as snapshot-shipping source node of " +
                            CtrlRscDfnApiCallHandler.getRscDfnDescription(rscDfnRef)
                    )
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "iterating through resources of " + CtrlRscDfnApiCallHandler.getRscDfnDescription(rscDfnRef),
                ApiConsts.FAIL_ACC_DENIED_SNAP_DFN
            );
        }
        return ret;
    }

    public Flux<ApiCallRc> shipSnapshot(
        String rscNameRef,
        String fromNodeNameRef,
        @Nullable String fromNicRef,
        String toNodeNameRef,
        String toNicRef,
        boolean runInBackgroundRef
    )
    {
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeModifyOperation(),
            Collections.emptyList(),
            rscNameRef,
            DUMMY_SNAPSHOT_SHIPPING_NAME
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Ship snapshot",
                lockGuardFactory.create() // TODO recheck required locks
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> shipSnapshotInTransaction(
                    rscNameRef,
                    fromNodeNameRef,
                    toNodeNameRef,
                    toNicRef,
                    runInBackgroundRef
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> shipSnapshotInTransaction(
        String rscNameRef,
        String fromNodeNameRef,
        String toNodeNameRef,
        @Nullable String toNicRef,
        boolean runInBackgroundRef
    )
    {
        ResourceConnection rscConn = rscConnHelper.loadOrCreateRscConn(
            null,
            fromNodeNameRef,
            toNodeNameRef,
            rscNameRef
        );
        SnapshotDefinition snapDfn = loadInProgressShipping(rscConn, rscNameRef);
        if (snapDfn != null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT_SHIPPING,
                    "Snapshot shipping of resource '" + rscNameRef + "' from node '" + fromNodeNameRef + "' to node '" +
                        toNodeNameRef + "' already in progress"
                )
            );
        }

        Resource fromRsc = ctrlApiDataLoader.loadRsc(fromNodeNameRef, rscNameRef, true);
        Resource toRsc = ctrlApiDataLoader.loadRsc(toNodeNameRef, rscNameRef, true);

        checkIfSnapshotShippingIsSupported(fromRsc, true);
        checkIfSnapshotShippingIsSupported(toRsc, false);

        ApiCallRcImpl responses = new ApiCallRcImpl();
        String snapShipName = getSnapshotNameForNextShipping(rscConn, responses);

        snapDfn = snapCrtHelper.createSnapshots(
            Arrays.asList(fromNodeNameRef, toNodeNameRef),
            LinstorParsingUtils.asRscName(rscNameRef),
            LinstorParsingUtils.asSnapshotName(snapShipName),
            responses
        );

        // TODO: delete "old enough" snapshots created by snapshot shipping

        Snapshot snapCurrentSource = getSnapshot(snapDfn, fromNodeNameRef);
        Snapshot snapPreviousSource = getPrevious(snapCurrentSource);
        Snapshot snapTarget = getSnapshot(snapDfn, toNodeNameRef);
        setShippingPropsAndFlags(snapPreviousSource, snapCurrentSource, snapTarget, toNicRef, toRsc);

        enableFlags(snapDfn, SnapshotDefinition.Flags.SHIPPING);

        ctrlTransactionHelper.commit();

        addDeprecationWarning(responses);

        return snapCrtHandler.postCreateSnapshot(snapDfn, runInBackgroundRef);
    }

    private void checkIfSnapshotShippingIsSupported(Resource rsc, boolean fromRsc)
    {
        try
        {
            boolean isInactiveFlagSet = rsc.getStateFlags().isSet(peerAccCtx.get(), Resource.Flags.INACTIVE);
            if (!isInactiveFlagSet && !fromRsc)
            {
                // toRsc must be inactive
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_SNAPSHOT_SHIPPING_TARGET,
                        "Snapshot shipping target must be inactive"
                    )
                );
            }

            Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc, peerAccCtx.get());
            for (StorPool sp : storPools)
            {
                DeviceProviderKind deviceProviderKind = sp.getDeviceProviderKind();
                if (!deviceProviderKind.isSnapshotShippingSupported())
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_SNAPSHOT_SHIPPING_NOT_SUPPORTED,
                            String.format(
                                "The storage pool kind %s does not support snapshot shipping",
                                deviceProviderKind.name()
                            )
                        )
                    );
                }
                ExtToolsManager extToolsManager = rsc.getNode().getPeer(peerAccCtx.get()).getExtToolsManager();
                checkRequiredExtTool(deviceProviderKind, extToolsManager, ExtTools.ZSTD, "zstd");
                checkRequiredExtTool(deviceProviderKind, extToolsManager, ExtTools.SOCAT, "socat");
                checkRequiredExtTool(
                    deviceProviderKind,
                    extToolsManager,
                    ExtTools.COREUTILS_LINUX,
                    "timeout from coreutils",
                    new ExtToolsInfo.Version(8, 5) // coreutils commit c403c31e8806b732e1164ef4a206b0eab71bca95
                );
                if (deviceProviderKind.equals(DeviceProviderKind.LVM_THIN))
                {
                    checkRequiredExtTool(
                        deviceProviderKind,
                        extToolsManager,
                        ExtTools.THIN_SEND_RECV,
                        "thin_send_recv",
                        new ExtToolsInfo.Version(0, 24)
                    );
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "checking if snapshot-shipping is supported by storage providers for resource: " +
                    CtrlRscApiCallHandler.getRscDescription(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }

    }

    private void checkRequiredExtTool(
        DeviceProviderKind deviceProviderKind,
        ExtToolsManager extToolsManagerRef,
        ExtTools extTool,
        String descr
    )
    {
        checkRequiredExtTool(deviceProviderKind, extToolsManagerRef, extTool, descr, null);
    }

    private void checkRequiredExtTool(
        DeviceProviderKind deviceProviderKind,
        ExtToolsManager extToolsManagerRef,
        ExtTools extTool,
        String toolDescr,
        @Nullable ExtToolsInfo.Version version
    )
    {
        ExtToolsInfo info = extToolsManagerRef.getExtToolInfo(extTool);
        if (info == null || !info.isSupported())
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_SNAPSHOT_SHIPPING_NOT_SUPPORTED,
                    String.format(
                        "%s based snapshot shipping requires support for %s",
                        deviceProviderKind.name(),
                        toolDescr
                    ),
                    true
                )
            );
        }
        if (version != null && !info.hasVersionOrHigher(version))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_SNAPSHOT_SHIPPING_NOT_SUPPORTED,
                    String.format(
                        "%s based snapshot shipping requires at least version %s for %s",
                        deviceProviderKind.name(),
                        version.toString(),
                        toolDescr
                    ),
                    true
                )
            );
        }
    }

    private void enableFlags(SnapshotDefinition snapDfnRef, SnapshotDefinition.Flags... flags)
    {
        try
        {
            snapDfnRef.getFlags().enableFlags(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "setting flags " + Arrays.asList(flags) + " on " +
                    CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline(snapDfnRef),
                ApiConsts.FAIL_ACC_DENIED_SNAP_DFN
            );
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
    }

    private @Nullable SnapshotDefinition loadInProgressShipping(
        ResourceConnection rscConn,
        String rscNameRef
    )
    {
        SnapshotName snapShipName = rscConn.getSnapshotShippingNameInProgress();
        SnapshotDefinition snapDfn = null;
        if (snapShipName != null)
        {
            snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapShipName.displayValue, true);
        }

        return snapDfn;
    }

    private String getSnapshotNameForNextShipping(ResourceConnection rscConnRef, ApiCallRcImpl responsesRef)
    {
        String snapShipName;
        ResourceDefinition rscDfn;
        try
        {
            rscDfn = rscConnRef.getSourceResource(peerAccCtx.get()).getResourceDefinition();


            String snapShipNamePrefix = new PriorityProps(
                propsHelper.getProps(rscDfn),
                propsHelper.getProps(rscDfn.getResourceGroup()),
                propsHelper.getCtrlPropsForView()
            ).getProp(ApiConsts.KEY_SNAPSHOT_SHIPPING_PREFIX, null, ApiConsts.DFLT_SNAPSHOT_SHIPPING_PREFIX);

            try
            {
                new SnapshotName(snapShipNamePrefix);
            }
            catch (InvalidNameException exc)
            {
                responsesRef.addEntries(
                    ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.WARN_INVLD_SNAPSHOT_SHIPPING_PREFIX,
                        String.format(
                            "'%s' is not a valid prefix for snapshot shipping. Defaulting to '%s'",
                            snapShipNamePrefix,
                            ApiConsts.DFLT_SNAPSHOT_SHIPPING_PREFIX
                        )
                    )
                );
                snapShipNamePrefix = ApiConsts.DFLT_SNAPSHOT_SHIPPING_PREFIX;
            }

            try
            {
                do
                {
                    int snapId = getNextSnapshotShippingId(rscDfn);
                    snapShipName = snapShipNamePrefix + Integer.toString(snapId);
                }
                while (rscDfn.getSnapshotDfn(peerAccCtx.get(), new SnapshotName(snapShipName)) != null);
            }
            catch (InvalidNameException exc)
            {
                throw new ImplementationError(exc);
            }

        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accessDeniedExc,
                "finding snapshot name for next snapshot-shipping",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }

        return snapShipName;
    }

    private int getNextSnapshotShippingId(ResourceDefinition rscDfnRef)
    {
        int ret;
        try
        {
            Props rscDfnProp = propsHelper.getProps(rscDfnRef);
            String val = rscDfnProp.getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_NEXT_ID);
            if (val == null)
            {
                ret = 0;
                rscDfnProp.setProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_NEXT_ID, "1");
            }
            else
            {
                ret = Integer.parseInt(val);
                rscDfnProp.setProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_NEXT_ID, Integer.toString(ret + 1));
            }
        }
        catch (InvalidKeyException | AccessDeniedException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        return ret;
    }

    private Snapshot getSnapshot(SnapshotDefinition snapDfnRef, String nodeNameRef)
    {
        Snapshot ret = null;
        try
        {
            ret = snapDfnRef.getSnapshot(peerAccCtx.get(), LinstorParsingUtils.asNodeName(nodeNameRef));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accessDeniedExc,
                "accassing snapshot of " + CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline(snapDfnRef),
                ApiConsts.FAIL_ACC_DENIED_SNAP_DFN
            );
        }
        return ret;
    }

    private @Nullable Snapshot getPrevious(Snapshot snapCurrentSourceRef)
    {
        Snapshot prevSourceSnapshot = null;
        try
        {
            ReadOnlyProps snapDfnProps = snapCurrentSourceRef.getSnapshotDefinition().getSnapDfnProps(peerAccCtx.get());
            String prevShippingName = snapDfnProps.getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_NAME_PREV);
            if (prevShippingName != null)
            {
                SnapshotDefinition prevSnapDfn = ctrlApiDataLoader.loadSnapshotDfn(
                    snapCurrentSourceRef.getResourceDefinition(),
                    new SnapshotName(prevShippingName),
                    true
                );

                prevSourceSnapshot = prevSnapDfn.getSnapshot(apiCtx, snapCurrentSourceRef.getNodeName());
            }
        }
        catch (AccessDeniedException | InvalidNameException exc)
        {
            // accessDenied of snapshots we _just_ created? sounds like a bug
            throw new ImplementationError(exc);
        }
        return prevSourceSnapshot;
    }

    private void setShippingPropsAndFlags(
        @Nullable Snapshot prevSnapSource,
        Snapshot snapSource,
        Snapshot snapTarget,
        @Nullable String toNicRef,
        Resource rscTarget
    )
    {
        try
        {
            assert Objects.equals(snapSource.getSnapshotDefinition(), snapTarget.getSnapshotDefinition());
            assert !Objects.equals(snapSource.getNode(), snapTarget.getNode());

            assert Objects.equals(rscTarget.getResourceDefinition(), snapTarget.getResourceDefinition());
            assert Objects.equals(rscTarget.getNode(), snapTarget.getNode());

            snapSource.getFlags().enableFlags(peerAccCtx.get(), Snapshot.Flags.SHIPPING_SOURCE);
            snapTarget.getFlags().enableFlags(peerAccCtx.get(), Snapshot.Flags.SHIPPING_TARGET);
            rscTarget.getStateFlags().enableFlags(peerAccCtx.get(), Resource.Flags.INACTIVE_PERMANENTLY);

            // TODO: find previous snapshot (if available) and set corresponding property

            SnapshotDefinition snapDfn = snapSource.getSnapshotDefinition();
            Props snapDfnProps = propsHelper.getProps(snapDfn, false);
            snapDfnProps.setProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_TARGET_NODE,
                snapTarget.getNode().getName().displayValue
            );
            snapDfnProps.setProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_SOURCE_NODE,
                snapSource.getNode().getName().displayValue
            );
            if (toNicRef != null)
            {
                snapDfnProps.setProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_PREF_TARGET_NIC, toNicRef);
            }

            for (SnapshotVolumeDefinition snapVlmDfn : snapDfn.getAllSnapshotVolumeDefinitions(apiCtx))
            {
                snapVlmDfn.getSnapVlmDfnProps(apiCtx)
                    .setProp(
                        InternalApiConsts.KEY_SNAPSHOT_SHIPPING_PORT,
                        Integer.toString(snapshotShippingPortPool.autoAllocate())
                    );
            }
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (AccessDeniedException exc)
        {
            // accessDenied of snapshots we _just_ created? sounds like a bug
            throw new ImplementationError(exc);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (ExhaustedPoolException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_POOL_EXHAUSTED_SNAPSHOT_SHIPPING_TCP_PORT,
                    "No TCP/IP port number could be allocated for snapshot shipping"
                ),
                exc
            );
        }
    }

    public ArrayList<SnapshotShippingListItemApi> listSnapshotShippings(
        List<String> nodeNamesRef,
        List<String> resourceNamesRef,
        List<String> snapshotnamesRef,
        List<String> statusRef
    )
    {
        ArrayList<SnapshotShippingListItemApi> ret = new ArrayList<>();

        Predicate<String> nodeNameFilter = createFilter(nodeNamesRef, String::toUpperCase);
        Predicate<ResourceName> rscNameFilter = createFilter(resourceNamesRef, LinstorParsingUtils::asRscName);
        Predicate<SnapshotName> snapNameFilter = createFilter(snapshotnamesRef, LinstorParsingUtils::asSnapshotName);

        ExceptionThrowingPredicate<StateFlags<SnapshotDefinition.Flags>, AccessDeniedException> running = ignored -> false;
        ExceptionThrowingPredicate<StateFlags<SnapshotDefinition.Flags>, AccessDeniedException> completed = ignored -> false;
        if (statusRef == null || statusRef.isEmpty())
        {
            running = flags -> flags.isSomeSet(
                peerAccCtx.get(),
                SnapshotDefinition.Flags.SHIPPING,
                SnapshotDefinition.Flags.SHIPPING_ABORT,
                SnapshotDefinition.Flags.SHIPPING_CLEANUP
            ) && !flags.isSet(peerAccCtx.get(), SnapshotDefinition.Flags.BACKUP);
            completed = flags -> flags.isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED);
        }
        else
        {
            for (String stat : statusRef)
            {
                if (ApiConsts.SnapshotShipStatus.RUNNING.getValue().equalsIgnoreCase(stat))
                {
                    running = flags -> flags.isSomeSet(
                        peerAccCtx.get(),
                        SnapshotDefinition.Flags.SHIPPING,
                        SnapshotDefinition.Flags.SHIPPING_ABORT,
                        SnapshotDefinition.Flags.SHIPPING_CLEANUP
                    ) && !flags.isSet(peerAccCtx.get(), SnapshotDefinition.Flags.BACKUP);
                }
                if (ApiConsts.SnapshotShipStatus.COMPLETE.getValue().equals(stat))
                {
                    completed = flags -> flags.isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED);
                }
            }
        }
        try
        {
            for (ResourceDefinition rscDfn : resourceDefinitionRepository.getMapForView(peerAccCtx.get()).values())
            {
                if (rscNameFilter.test(rscDfn.getName()))
                {
                    for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(peerAccCtx.get()))
                    {
                        if (snapNameFilter.test(snapshotDfn.getName()))
                        {
                            try
                            {
                                StateFlags<Flags> snapDfnFlags = snapshotDfn.getFlags();
                                boolean isShippmentInProgress = running.test(snapDfnFlags);
                                if (isShippmentInProgress || completed.test(snapDfnFlags))
                                {
                                    final SnapshotDefinitionListItemApi snapItem = snapshotDfn
                                        .getListItemApiData(peerAccCtx.get());

                                    ReadOnlyProps snapDfnProps = snapshotDfn.getSnapDfnProps(peerAccCtx.get());
                                    String sourceNode = snapDfnProps
                                        .getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_SOURCE_NODE);
                                    String targetNode = snapDfnProps
                                        .getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_TARGET_NODE);

                                    if (nodeNameFilter.test(sourceNode.toUpperCase()) ||
                                        nodeNameFilter.test(targetNode.toUpperCase()))
                                    {
                                        ret.add(
                                            new SnapshotShippingListItemPojo(
                                                snapItem,
                                                sourceNode,
                                                targetNode,
                                                isShippmentInProgress ?
                                                    ApiConsts.SnapshotShipStatus.RUNNING.getValue() :
                                                    ApiConsts.SnapshotShipStatus.COMPLETE.getValue()
                                            )
                                        );
                                    }
                                }
                            }
                            catch (AccessDeniedException accDeniedExc)
                            {
                                // don't add snapshot definition without access
                            }
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
        }
        return ret;
    }

    private <T> Predicate<T> createFilter(List<String> list, Function<String, T> mappingFkt)
    {
        Predicate<T> predicate;
        if (list == null || list.isEmpty())
        {
            predicate = ignored -> true;
        }
        else
        {
            final Set<T> filteredList = list.stream().map(mappingFkt)
                .collect(Collectors.toSet());
            predicate = filteredList::contains;
        }

        return predicate;
    }

    public static void addDeprecationWarning(ApiCallRcImpl apiCallRcRef)
    {
        apiCallRcRef.add(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_DEPRECATED,
                "Snapshot-shipping is deprecated and will be deleted in a future LINSTOR release." +
                    " Use Backup-shipping instead"
            )
        );
    }
}
