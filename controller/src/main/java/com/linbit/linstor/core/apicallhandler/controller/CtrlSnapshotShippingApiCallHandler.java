package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.makeSnapshotContext;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.Collections;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlSnapshotShippingApiCallHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSnapshotHelper ctrlSnapshotHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlRscConnectionHelper rscConnHelper;
    private final CtrlPropsHelper propsHelper;

    private final CtrlSnapshotCrtHelper snapCrtHelper;
    private final CtrlSnapshotCrtApiCallHandler snapCrtHandler;
    private final CtrlSnapshotDeleteApiCallHandler snapDelHandler;
    private final DynamicNumberPool snapshotShippingPortPool;

    @Inject
    public CtrlSnapshotShippingApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSnapshotHelper ctrlSnapshotHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlRscConnectionHelper rscConnHelperRef,
        CtrlSnapshotCrtHelper snapCrtHelperRef,
        CtrlSnapshotCrtApiCallHandler snapCrtHandlerRef,
        CtrlSnapshotDeleteApiCallHandler snapDelHandlerRef,
        CtrlPropsHelper propsHelperRef,
        @Named(
            NumberPoolModule.SNAPSHOPT_SHIPPING_PORT_POOL
        ) DynamicNumberPool snapshotShippingPortPoolRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSnapshotHelper = ctrlSnapshotHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        peerAccCtx = peerAccCtxRef;
        lockGuardFactory = lockGuardFactoryRef;
        rscConnHelper = rscConnHelperRef;
        snapCrtHelper = snapCrtHelperRef;
        snapCrtHandler = snapCrtHandlerRef;
        snapDelHandler = snapDelHandlerRef;
        propsHelper = propsHelperRef;
        snapshotShippingPortPool = snapshotShippingPortPoolRef;
    }

    public Flux<ApiCallRc> shipSnapshot(
        String rscNameRef,
        String fromNodeNameRef,
        String fromNicRef,
        String toNodeNameRef,
        String toNicRef
    )
    {
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeModifyOperation(),
            Collections.emptyList(),
            rscNameRef,
            ApiConsts.VAL_SNAP_SHIP_NAME
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Ship snapshot",
                lockGuardFactory.create() // TODO recheck required locks
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> shipSnapshotInTransaction(rscNameRef, fromNodeNameRef, fromNicRef, toNodeNameRef, toNicRef)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> shipSnapshotInTransaction(
        String rscNameRef,
        String fromNodeNameRef,
        String fromNicRef,
        String toNodeNameRef,
        String toNicRef
    )
    {
        ResourceConnection rscConn = rscConnHelper.loadOrCreateRscConn(
            null,
            fromNodeNameRef,
            toNodeNameRef,
            rscNameRef
        );
        SnapshotDefinition snapDfn = loadInProgressShipping(rscConn, rscNameRef, fromNodeNameRef, toNodeNameRef);
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

        ApiCallRcImpl responses = new ApiCallRcImpl();
        String snapShipName = getSnapshotNameForNextShipping(rscConn, responses);

        snapDfn = snapCrtHelper.createSnapshots(
            Arrays.asList(fromNodeNameRef, toNodeNameRef),
            rscNameRef,
            snapShipName,
            responses
        );

        // TODO: delete "old enough" snapshots created by snapshot shipping

        Snapshot snapCurrentSource = getSnapshot(snapDfn, fromNodeNameRef);
        Snapshot snapPreviousSource = getPrevious(snapCurrentSource);
        Snapshot snapTarget = getSnapshot(snapDfn, toNodeNameRef);
        setShippingPropsAndFlags(snapPreviousSource, snapCurrentSource, snapTarget);

        enableFlags(snapDfn, SnapshotDefinition.Flags.SHIPPING);

        ctrlTransactionHelper.commit();

        return snapCrtHandler.postCreateSnapshot(snapDfn);
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
                "setting flags " + Arrays.asList(flags) + " on " + CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline(snapDfnRef),
                ApiConsts.FAIL_ACC_DENIED_SNAP_DFN
            );
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
    }

    private SnapshotDefinition loadInProgressShipping(
        ResourceConnection rscConn,
        String rscNameRef,
        String fromNodeNameRef,
        String toNodeNameRef
    )
    {
        String snapShipName = null;
        try
        {
            snapShipName = rscConn.getProps(peerAccCtx.get()).getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_NAME_IN_PROGRESS);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "get in progress snapshot-shipping of resource '" + rscNameRef +
                    "' from node '" + fromNodeNameRef + "' to node '" + toNodeNameRef +
                    "'",
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
        }
        SnapshotDefinition snapDfn = null;
        if (snapShipName != null)
        {
            snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapShipName, true);
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
                propsHelper.getCtrlPropsForView()
            ).getProp(ApiConsts.KEY_SNAPSHOT_SHIPPING_PREFIX, null, ApiConsts.DFLT_SNAPSHOT_SHIPPING_PREFIX);

            try
            {
                new SnapshotName(snapShipNamePrefix);
            }
            catch (InvalidNameException exc) {
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

    private Snapshot getPrevious(Snapshot snapCurrentSourceRef)
    {
        Snapshot prevSourceSnapshot = null;
        try
        {
            Props snapDfnProps = snapCurrentSourceRef.getSnapshotDefinition().getProps(peerAccCtx.get());
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

    private void setShippingPropsAndFlags(@Nullable Snapshot prevSnapSource, Snapshot snapSource, Snapshot snapTarget)
    {
        try
        {
            snapSource.getFlags().enableFlags(peerAccCtx.get(), Snapshot.Flags.SHIPPING_SOURCE);
            snapTarget.getFlags().enableFlags(peerAccCtx.get(), Snapshot.Flags.SHIPPING_TARGET);

            // TODO: find previous snapshot (if available) and set corresponding property

            Props snapDfnProps = propsHelper.getProps(snapSource.getSnapshotDefinition());
            snapDfnProps.setProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_TARGET_NODE,
                snapTarget.getNode().getName().displayValue
            );
            snapDfnProps.setProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_SOURCE_NODE,
                snapSource.getNode().getName().displayValue
            );

            snapDfnProps.setProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_PORT,
                Integer.toString(snapshotShippingPortPool.autoAllocate())
            );
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
}
