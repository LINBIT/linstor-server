package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collection;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlSnapshotShippingAbortHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final Provider<CtrlSnapshotDeleteApiCallHandler> snapDelHandlerProvider;

    @Inject
    public CtrlSnapshotShippingAbortHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockguardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        Provider<CtrlSnapshotDeleteApiCallHandler> snapDelHandlerProviderRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockguardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        snapDelHandlerProvider = snapDelHandlerProviderRef;
    }

    public Flux<ApiCallRc> abortSnapshotShippingPrivileged(Node nodeRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Delete snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> abortSnapshotShippingPrivilegedInTransaction(nodeRef)
            );
    }

    private Flux<ApiCallRc> abortSnapshotShippingPrivilegedInTransaction(Node nodeRef)
    {
        Flux<ApiCallRc> flux = Flux.empty();
        try
        {

            for (Snapshot snap : nodeRef.getSnapshots(apiCtx))
            {
                SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
                if (snapDfn.getFlags().isSet(apiCtx, SnapshotDefinition.Flags.SHIPPING))
                {
                    flux = flux.concatWith(snapDelHandlerProvider.get()
                        .deleteSnapshot(snapDfn.getResourceName().displayValue, snapDfn.getName().displayValue)
                    );
                }
            }
            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return flux;

    }

    public void markSnapshotShippingAborted(SnapshotDefinition snapDfnRef)
    {
        try
        {
            snapDfnRef.getFlags().disableFlags(
                apiCtx,
                SnapshotDefinition.Flags.SHIPPING,
                SnapshotDefinition.Flags.SHIPPING_CLEANUP,
                SnapshotDefinition.Flags.SHIPPED
            );
            snapDfnRef.getFlags().enableFlags(apiCtx, SnapshotDefinition.Flags.SHIPPING_ABORT);
            ResourceName rscName = snapDfnRef.getResourceName();

            Collection<Snapshot> snapshots = snapDfnRef.getAllSnapshots(apiCtx);
            for (Snapshot snap : snapshots)
            {
                Resource rsc1 = snap.getNode().getResource(apiCtx, rscName);
                for (Snapshot snap2 : snapshots)
                {
                    if (snap != snap2)
                    {
                        Resource rsc2 = snap2.getNode().getResource(apiCtx, rscName);

                        ResourceConnection rscConn = rsc1.getAbsResourceConnection(apiCtx, rsc2);
                        if (rscConn != null)
                        {
                            rscConn.setSnapshotShippingNameInProgress(null);
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }
}
