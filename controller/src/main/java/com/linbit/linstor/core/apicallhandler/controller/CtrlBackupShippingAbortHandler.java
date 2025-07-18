package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.BackupToS3;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.BackupInfoManager.AbortInfo;
import com.linbit.linstor.core.BackupInfoManager.AbortS3Info;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlBackupShippingAbortHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final Provider<CtrlSnapshotDeleteApiCallHandler> snapDelHandlerProvider;
    private final BackupInfoManager backupInfoMgr;
    private final BackupToS3 backupHandler;
    private final RemoteRepository remoteRepo;
    private final ErrorReporter errorReporter;
    private final CtrlSecurityObjects ctrlSecObj;
    private final Provider<CtrlRemoteApiCallHandler> ctrlRemoteApiCallHandler;

    @Inject
    public CtrlBackupShippingAbortHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockguardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        Provider<CtrlSnapshotDeleteApiCallHandler> snapDelHandlerProviderRef,
        BackupInfoManager backupInfoMgrRef,
        BackupToS3 backupHandlerRef,
        RemoteRepository remoteRepoRef,
        ErrorReporter errorReporterRef,
        CtrlSecurityObjects ctrlSecObjRef,
        Provider<CtrlRemoteApiCallHandler> ctrlRemoteApiCallHandlerRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockguardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        snapDelHandlerProvider = snapDelHandlerProviderRef;
        backupInfoMgr = backupInfoMgrRef;
        backupHandler = backupHandlerRef;
        remoteRepo = remoteRepoRef;
        errorReporter = errorReporterRef;
        ctrlSecObj = ctrlSecObjRef;
        ctrlRemoteApiCallHandler = ctrlRemoteApiCallHandlerRef;
    }

    public Flux<ApiCallRc> abortAllShippingPrivileged(Node nodeRef, boolean abortMultiPartRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Abort all snapshot shipments to node",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> abortAllShippingPrivilegedInTransaction(nodeRef, abortMultiPartRef)
            );
    }

    private Flux<ApiCallRc> abortAllShippingPrivilegedInTransaction(Node nodeRef, boolean abortMultiPartRef)
    {
        Flux<ApiCallRc> flux = Flux.empty();
        try
        {
            for (Snapshot snap : nodeRef.getSnapshots(apiCtx))
            {
                SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
                if (
                    BackupShippingUtils.isAnyShippingInProgress(snapDfn, apiCtx) ||
                        BackupShippingUtils.isAnyAbortInProgress(snapDfn, apiCtx)
                )
                {
                    flux = flux.concatWith(abortBackupShippings(snapDfn, abortMultiPartRef));
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

    public Flux<ApiCallRc> abortBackupShippingPrivileged(SnapshotDefinition snapDfn, boolean abortMultiPartRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Abort backup shipments of rscDfn",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> abortBackupShippingPrivilegedInTransaction(snapDfn, abortMultiPartRef)
            );
    }

    private Flux<ApiCallRc> abortBackupShippingPrivilegedInTransaction(
        SnapshotDefinition snapDfn,
        boolean abortMultiPartRef
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        flux = flux.concatWith(abortBackupShippings(snapDfn, abortMultiPartRef));
        ctrlTransactionHelper.commit();
        return flux;
    }

    private Flux<ApiCallRc> abortBackupShippings(SnapshotDefinition snapDfn, boolean abortMultiPartRef)
    {
        Flux<ApiCallRc> flux = Flux.empty();
        boolean shouldAbort = false;
        try
        {
            for (Snapshot snap : snapDfn.getAllSnapshots(apiCtx))
            {
                Map<SnapshotDefinition.Key, AbortInfo> abortEntries = backupInfoMgr.abortCreateGetEntries(
                    snap.getNodeName()
                );
                if (abortEntries != null && !abortEntries.isEmpty())
                {
                    AbortInfo abortInfo = abortEntries.get(snapDfn.getSnapDfnKey());
                    if (abortInfo != null && !abortInfo.isEmpty())
                    {
                        shouldAbort = true;
                        if (abortMultiPartRef)
                        {
                            List<AbortS3Info> abortS3List = abortInfo.abortS3InfoList;
                            for (AbortS3Info abortS3Info : abortS3List)
                            {
                                try
                                {
                                    S3Remote remote = remoteRepo.getS3(apiCtx, new RemoteName(abortS3Info.remoteName));
                                    byte[] masterKey = ctrlSecObj.getCryptKey();
                                    if (masterKey == null || masterKey.length == 0)
                                    {
                                        throw new ApiRcException(
                                            ApiCallRcImpl
                                                .entryBuilder(
                                                    ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY,
                                                    "Unable to decrypt the S3 access key and secret key " +
                                                        "without having a master key"
                                                )
                                                .setCause("The masterkey was not initialized yet")
                                                .setCorrection("Create or enter the master passphrase")
                                                .build()
                                        );
                                    }
                                    backupHandler.abortMultipart(
                                        abortS3Info.backupName,
                                        abortS3Info.uploadId,
                                        remote,
                                        apiCtx,
                                        masterKey
                                    );
                                }
                                catch (SdkClientException exc)
                                {
                                    if (exc.getClass() == AmazonS3Exception.class)
                                    {
                                        AmazonS3Exception s3Exc = (AmazonS3Exception) exc;
                                        if (s3Exc.getStatusCode() != 404)
                                        {
                                            errorReporter.reportError(exc);
                                        }
                                    }
                                    else
                                    {
                                        errorReporter.reportError(exc);
                                    }
                                }
                                catch (InvalidNameException exc)
                                {
                                    throw new ImplementationError(exc);
                                }
                            }
                            // nothing to do for AbortL2LInfo entries, just enable the ABORT flag
                        }
                        flux = flux.concatWith(
                            ctrlRemoteApiCallHandler.get()
                                .cleanupRemotesIfNeeded(
                                backupInfoMgr.abortCreateDeleteEntries(snap.getNodeName(), snapDfn.getSnapDfnKey())
                            )
                        );
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        if (shouldAbort)
        {
            enableAbortingPrivileged(
                snapDfn
            );
            flux = flux.concatWith(
                snapDelHandlerProvider.get()
                    .deleteSnapshot(
                        snapDfn.getResourceName(),
                        snapDfn.getName(),
                        null
                    )
            );
        }
        return flux;
    }

    private void enableAbortingPrivileged(SnapshotDefinition snapDfn)
    {
        try
        {
            @Nullable Props backupProps = snapDfn.getSnapDfnProps(apiCtx)
                .getNamespace(ApiConsts.NAMESPC_BACKUP_SHIPPING);
            // if the decision was made that we need to abort, this namespc should not be able to be null, but check
            // anyways
            if (backupProps != null)
            {
                @Nullable Props dstProps = backupProps.getNamespace(InternalApiConsts.KEY_BACKUP_TARGET);
                if (
                    dstProps != null && BackupShippingUtils.hasShippingStatus(
                        snapDfn,
                        null,
                        InternalApiConsts.VALUE_SHIPPING,
                        apiCtx
                    )
                )
                {
                    dstProps.setProp(InternalApiConsts.KEY_SHIPPING_STATUS, InternalApiConsts.VALUE_ABORTING);
                }
                else
                {
                    @Nullable Props srcProps = backupProps.getNamespace(InternalApiConsts.KEY_BACKUP_SOURCE);
                    if (srcProps != null)
                    {
                        Iterator<String> namespcIter = srcProps.iterateNamespaces();
                        while (namespcIter.hasNext())
                        {
                            String remoteName = namespcIter.next();
                            if (
                                BackupShippingUtils.hasShippingStatus(
                                    snapDfn,
                                    remoteName,
                                    InternalApiConsts.VALUE_SHIPPING,
                                    apiCtx
                                )
                            )
                            {
                                srcProps.setProp(
                                    InternalApiConsts.KEY_SHIPPING_STATUS,
                                    InternalApiConsts.VALUE_ABORTING,
                                    remoteName
                                );
                            }
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException | InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }
}
