package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.BackupToS3;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.api.pojo.backups.BackupMetaInfoPojo;
import com.linbit.linstor.backupshipping.S3MetafileNameInfo;
import com.linbit.linstor.backupshipping.S3VolumeNameInfo;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlBackupApiHelper
{
    private final CtrlSecurityObjects ctrlSecObj;
    private final RemoteRepository remoteRepo;
    private final Provider<AccessContext> peerAccCtx;
    private final BackupToS3 backupHandler;
    private final ErrorReporter errorReporter;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final AccessContext sysCtx;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;

    @Inject
    public CtrlBackupApiHelper(
        CtrlSecurityObjects ctrlSecObjRef,
        RemoteRepository remoteRepoRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        BackupToS3 backupHandlerRef,
        ErrorReporter errorReporterRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        @SystemContext AccessContext sysCtxRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef
    )
    {
        ctrlSecObj = ctrlSecObjRef;
        remoteRepo = remoteRepoRef;
        peerAccCtx = peerAccCtxRef;
        backupHandler = backupHandlerRef;
        errorReporter = errorReporterRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        sysCtx = sysCtxRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;

    }

    /**
     * Check that the masterKey exists and is unlocked, then returns it.
     */
    byte[] getLocalMasterKey()
    {
        byte[] masterKey = ctrlSecObj.getCryptKey();
        if (masterKey == null || masterKey.length == 0)
        {
            throw new ApiRcException(
                ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY,
                        "Unable to decrypt the S3 access key and secret key without having a master key"
                    )
                    .setCause("The masterkey was not initialized yet")
                    .setCorrection("Create or enter the master passphrase")
                    .build()
            );
        }
        return masterKey;
    }

    /**
     * Get the remote with the given name only if it is an s3 remote
     */
    S3Remote getS3Remote(String remoteName) throws AccessDeniedException, InvalidNameException
    {
        AbsRemote remote = getRemote(remoteName);
        if (!(remote instanceof S3Remote))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME | ApiConsts.MASK_BACKUP,
                    "The remote " + remoteName + " is not an s3 remote."
                )
            );
        }
        return (S3Remote) remote;
    }

    /**
     * Get the remote with the given name only if it is a l2l-remote
     */
    LinstorRemote getL2LRemote(String remoteName) throws AccessDeniedException, InvalidNameException
    {
        AbsRemote remote = getRemote(remoteName);
        if (!(remote instanceof LinstorRemote))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME | ApiConsts.MASK_BACKUP,
                    "The remote " + remoteName + " is not a linstor remote."
                )
            );
        }
        return (LinstorRemote) remote;
    }

    /**
     * Get the remote with the given name and make sure it exists.
     */
    AbsRemote getRemote(String remoteName) throws AccessDeniedException, InvalidNameException
    {
        if (remoteName == null || remoteName.isEmpty())
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME | ApiConsts.MASK_BACKUP,
                    "No remote name was given. Please provide a valid remote name."
                )
            );
        }
        AbsRemote remote = null;
        remote = remoteRepo.get(peerAccCtx.get(), new RemoteName(remoteName, true));
        if (remote == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_REMOTE | ApiConsts.MASK_BACKUP,
                    "The remote " + remoteName + " does not exist. Please provide a valid remote or create a new one."
                )
            );
        }
        return remote;
    }

    /**
     * throws an exception if the remote is marked for deletion, since those remotes are not allowed to start new
     * shipments
     *
     * @param remote
     *
     * @return
     *
     * @throws AccessDeniedException
     */
    void ensureShippingToRemoteAllowed(AbsRemote remote) throws AccessDeniedException
    {
        if (remote.getFlags().isSomeSet(peerAccCtx.get(), AbsRemote.Flags.MARK_DELETED, AbsRemote.Flags.DELETE))
        {
            throw new ApiException(
                "The given remote " + remote +
                    " is marked for deletion and therefore not allowed to start a new restore."
            );
        }
    }

    /**
     * Get all snapDfns that are currently shipping a backup.
     */
    Set<SnapshotDefinition> getInProgressBackups(ResourceDefinition rscDfn)
        throws AccessDeniedException, InvalidNameException
    {
        return getInProgressBackups(rscDfn, null);
    }

    Set<SnapshotDefinition> getInProgressBackups(ResourceDefinition rscDfn, @Nullable AbsRemote remote)
        throws AccessDeniedException, InvalidNameException
    {
        Set<SnapshotDefinition> snapDfns = new HashSet<>();
        for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(peerAccCtx.get()))
        {
            if (
                snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING) &&
                    snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.BACKUP)
            )
            {
                if (remote == null)
                {
                    snapDfns.add(snapDfn);
                }
                else
                {
                    for (Snapshot snap : snapDfn.getAllSnapshots(peerAccCtx.get()))
                    {
                        String remoteName = "";
                        if (snap.getFlags().isSet(peerAccCtx.get(), Snapshot.Flags.BACKUP_SOURCE))
                        {
                            remoteName = snap.getProps(peerAccCtx.get())
                                .getProp(InternalApiConsts.KEY_BACKUP_TARGET_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                        }
                        else if (snap.getFlags().isSet(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET))
                        {
                            remoteName = snap.getProps(peerAccCtx.get())
                                .getProp(InternalApiConsts.KEY_BACKUP_SRC_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                        }

                        if (hasShippingToRemote(remoteName, remote.getName().displayValue))
                        {
                            snapDfns.add(snapDfn);
                            break;
                        }
                    }
                }
            }
        }
        return snapDfns;
    }

    boolean hasShippingToRemote(String remoteToCheck, String expectedRemote)
        throws AccessDeniedException, InvalidNameException
    {
        boolean ret = expectedRemote != null;
        if (ret && !expectedRemote.equalsIgnoreCase(remoteToCheck))
        {
            AbsRemote remote = remoteRepo.get(sysCtx, new RemoteName(remoteToCheck, true));
            if (remote instanceof StltRemote)
            {
                // we checked the stlt-remote instead of the correct remote, check again
                if (!((StltRemote) remote).getLinstorRemoteName().displayValue.equalsIgnoreCase(expectedRemote))
                {
                    // the correct remote doesn't have the same name either
                    ret = false;
                }
                // the correct remote had the same name, ret stays true
            }
            else if (remote instanceof S3Remote)
            {
                // it already is the correct remote, and the name is not the same
                ret = false;
            }
        }
        // the remote has the same name, ret stays true
        return ret;
    }

    /**
     * Get all s3Keys of the given remote, filtered by rscName
     */
    Set<String> getAllS3Keys(S3Remote s3Remote, String rscName) throws AccessDeniedException
    {
        List<S3ObjectSummary> objects = backupHandler.listObjects(
            rscName,
            s3Remote,
            peerAccCtx.get(),
            getLocalMasterKey()
        );
        // get ALL s3 keys of the given bucket, including possibly not linstor related ones
        return objects.stream()
            .map(S3ObjectSummary::getKey)
            .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Get all s3 objects that can be verified to have been created by linstor
     */
    Map<String, S3ObjectInfo> loadAllLinstorS3Objects(
        S3Remote s3RemoteRef,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException
    {
        Set<String> allS3Keys = getAllS3Keys(s3RemoteRef, null);
        Map<String, S3ObjectInfo> ret = new TreeMap<>();
        // add all backups to the list that have usable metadata-files
        for (String s3Key : allS3Keys)
        {
            try
            {
                S3MetafileNameInfo info = new S3MetafileNameInfo(s3Key);
                // throws parse exception if not linstor json
                BackupMetaDataPojo s3MetaFile = backupHandler.getMetaFile(
                    s3Key,
                    s3RemoteRef,
                    peerAccCtx.get(),
                    getLocalMasterKey()
                );
                S3ObjectInfo metaInfo = ret.computeIfAbsent(s3Key, S3ObjectInfo::new);
                metaInfo.exists = true;
                metaInfo.metaFile = s3MetaFile;
                for (List<BackupMetaInfoPojo> backupInfoPojoList : s3MetaFile.getBackups().values())
                {
                    for (BackupMetaInfoPojo backupInfoPojo : backupInfoPojoList)
                    {
                        String childS3Key = backupInfoPojo.getName();
                        S3ObjectInfo childS3Obj = ret.computeIfAbsent(childS3Key, S3ObjectInfo::new);
                        childS3Obj.referencedBy.add(metaInfo);
                        metaInfo.references.add(childS3Obj);
                    }
                }

                SnapshotDefinition snapDfn = loadSnapDfnIfExists(info.rscName, info.snapName);
                if (snapDfn != null)
                {
                    if (snapDfn.getUuid().toString().equals(s3MetaFile.getSnapDfnUuid()))
                    {
                        metaInfo.snapDfn = snapDfn;
                    }
                    else
                    {
                        apiCallRc.addEntry(
                            "Not marking SnapshotDefinition " + info.rscName + " / " + info.snapName +
                                " for exclusion as the UUID does not match with the backup",
                            ApiConsts.WARN_NOT_FOUND
                        );
                    }
                }

                String basedOnS3Key = s3MetaFile.getBasedOn();
                if (basedOnS3Key != null)
                {
                    S3ObjectInfo basedOnS3MetaInfo = ret.computeIfAbsent(basedOnS3Key, S3ObjectInfo::new);
                    basedOnS3MetaInfo.referencedBy.add(metaInfo);
                    metaInfo.references.add(basedOnS3MetaInfo);
                }
            }
            catch (MismatchedInputException exc)
            {
                // ignore, most likely an older format of linstor's backup .meta json
            }
            catch (IOException exc)
            {
                String errRepId = errorReporter.reportError(exc, peerAccCtx.get(), null, "used s3 key: " + s3Key);
                apiCallRc.addEntry(
                    "IO exception while parsing metafile " + s3Key + ". Details in error report " + errRepId,
                    ApiConsts.FAIL_UNKNOWN_ERROR
                );
            }
            catch (ParseException ignored)
            {
                // Ignored, not a meta file
            }

            try
            {
                S3VolumeNameInfo info = new S3VolumeNameInfo(s3Key);
                S3ObjectInfo s3DataInfo = ret.computeIfAbsent(s3Key, S3ObjectInfo::new);
                s3DataInfo.exists = true;
                s3DataInfo.snapDfn = loadSnapDfnIfExists(info.rscName, info.snapName);
            }
            catch (ParseException ignored)
            {
                // Ignored, not a volume file
            }
        }

        return ret;
    }

    /**
     * Unlike {@link CtrlApiDataLoader#loadSnapshotDfn(String, String, boolean)} this method does not expect rscDfn to
     * exist when trying to load snapDfn
     *
     * @param rscName
     * @param snapName
     *
     * @return
     */
    SnapshotDefinition loadSnapDfnIfExists(String rscName, String snapName)
    {
        SnapshotDefinition snapDfn = null;
        try
        {
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, false);
            if (rscDfn != null)
            {
                snapDfn = rscDfn.getSnapshotDfn(
                    peerAccCtx.get(),
                    new SnapshotName(snapName)
                );
            }
        }
        catch (InvalidNameException | AccessDeniedException ignored)
        {
        }
        return snapDfn;
    }

    /**
     * Removes the snap from the "shipping started" list on the stlt. This action can't be done
     * by the stlt itself since that might be too early and therefore trigger a second shipping by an
     * unrelated update
     */
    public Flux<ApiCallRc> startStltCleanup(Peer peer, String rscNameRef, String snapNameRef)
    {
        byte[] msg = ctrlStltSerializer.headerlessBuilder().notifyBackupShippingFinished(rscNameRef, snapNameRef)
            .build();
        return peer.apiCall(InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_FINISHED, msg).map(
            inputStream -> CtrlSatelliteUpdateCaller.deserializeApiCallRc(
                peer.getNode().getName(),
                inputStream
            )
        );
    }

    public Flux<ApiCallRc> cleanupStltRemote(StltRemote remote)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Cleanup Stlt-Remote",
                lockGuardFactory.create()
                    .write(LockObj.REMOTE_MAP).buildDeferred(),
                () -> cleanupStltRemoteInTransaction(remote)
            );
    }

    private Flux<ApiCallRc> cleanupStltRemoteInTransaction(StltRemote remote)
    {
        Flux<ApiCallRc> flux;
        try
        {
            remote.getFlags().enableFlags(sysCtx, StltRemote.Flags.DELETE);
            ctrlTransactionHelper.commit();
            flux = ctrlSatelliteUpdateCaller.updateSatellites(remote)
                .concatWith(
                    scopeRunner.fluxInTransactionalScope(
                        "Removing temporary satellite remote",
                        lockGuardFactory.create()
                            .write(LockObj.REMOTE_MAP).buildDeferred(),
                        () -> deleteStltRemoteInTransaction(remote.getName().displayValue)
                    )
                );
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        return flux;
    }

    private Flux<ApiCallRc> deleteStltRemoteInTransaction(String remoteNameRef)
    {
        AbsRemote remote;
        try
        {
            remote = remoteRepo.get(sysCtx, new RemoteName(remoteNameRef, true));
            if (!(remote instanceof StltRemote))
            {
                throw new ImplementationError("This method should only be called for satellite remotes");
            }
            remoteRepo.remove(sysCtx, remote.getName());
            remote.delete(sysCtx);

            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException | InvalidNameException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        return Flux.empty();
    }

    S3MetafileNameInfo getLatestBackup(Set<String> s3keys, String snapName)
    {
        S3MetafileNameInfo latest = null;
        for (String key : s3keys)
        {
            try
            {
                S3MetafileNameInfo current = new S3MetafileNameInfo(key);
                if (snapName != null && !snapName.isEmpty() && !snapName.equals(current.snapName))
                {
                    // Snapshot names do not match, ignore this backup
                    continue;
                }
                if (latest == null || latest.backupTime.isBefore(current.backupTime))
                {
                    latest = current;
                }
            }
            catch (ParseException ignored)
            {
                // Not a metadata file, ignore
            }
        }
        return latest;
    }

    static class S3ObjectInfo
    {
        private String s3Key;
        private boolean exists = false;
        private BackupMetaDataPojo metaFile;
        private SnapshotDefinition snapDfn = null;
        private HashSet<S3ObjectInfo> referencedBy = new HashSet<>();
        private HashSet<S3ObjectInfo> references = new HashSet<>();

        public S3ObjectInfo(String s3KeyRef)
        {
            s3Key = s3KeyRef;
        }

        public boolean isMetaFile()
        {
            return metaFile != null;
        }

        public SnapshotDefinition getSnapDfn()
        {
            return snapDfn;
        }

        public boolean doesExist()
        {
            return exists;
        }

        public BackupMetaDataPojo getMetaFile()
        {
            return metaFile;
        }

        public String getS3Key()
        {
            return s3Key;
        }

        public HashSet<S3ObjectInfo> getReferencedBy()
        {
            return referencedBy;
        }

        public HashSet<S3ObjectInfo> getReferences()
        {
            return references;
        }

        @Override
        public String toString()
        {
            return "S3ObjectInfo [s3Key=" + s3Key + "]";
        }
    }
}
