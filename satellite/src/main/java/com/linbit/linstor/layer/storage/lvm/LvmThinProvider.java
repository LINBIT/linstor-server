package com.linbit.linstor.layer.storage.lvm;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LvmVolumeType;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.layer.storage.utils.StorageConfigReader;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.MkfsUtils;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

@Singleton
public class LvmThinProvider extends LvmProvider
{
    @Inject
    public LvmThinProvider(
        ErrorReporter errorReporter,
        ExtCmdFactoryStlt extCmdFactory,
        @DeviceManagerContext AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        SnapshotShippingService snapShipMrgRef,
        StltExtToolsChecker extToolsCheckerRef,
        CloneService cloneServiceRef,
        BackupShippingMgr backupShipMgrRef
    )
    {
        super(
            errorReporter,
            extCmdFactory,
            storDriverAccCtx,
            stltConfigAccessor,
            wipeHandler,
            notificationListenerProvider,
            transMgrProvider,
            "LVM-Thin",
            DeviceProviderKind.LVM_THIN,
            snapShipMrgRef,
            extToolsCheckerRef,
            cloneServiceRef,
            backupShipMgrRef
        );
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.LVM_THIN;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void updateInfo(LvmData<?> vlmDataRef, LvsInfo infoRef)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        super.updateInfo(vlmDataRef, infoRef);
        LvmThinData<?> lvmThinData = (LvmThinData<?>) vlmDataRef;
        if (infoRef == null)
        {
            lvmThinData.setThinPool(getThinPool(vlmDataRef.getStorPool()));
            lvmThinData.setAllocatedPercent(0);
        }
        else
        {
            lvmThinData.setThinPool(infoRef.thinPool);
            lvmThinData.setAllocatedPercent(infoRef.dataPercent / 100.0f);
        }

        if (vlmDataRef.exists() && vlmDataRef.getVolume() instanceof Volume &&
            !isCloning((LvmData<Resource>) vlmDataRef))
        {
            // update allocated size after we set the dataPercent
            updateAllocatedSize((LvmThinData<Resource>) vlmDataRef);
        }
    }

    @Override
    protected void createLvImpl(LvmData<Resource> lvmVlmData) throws StorageException, AccessDeniedException
    {
        LvmThinData<Resource> vlmData = (LvmThinData<Resource>) lvmVlmData;
        String volumeGroup = vlmData.getVolumeGroup();
        String lvId = asLvIdentifier(vlmData);

        List<String> additionalOptions = MkfsUtils.shellSplit(getLvcreateOptions(vlmData));
        String[] additionalOptionsArr = new String[additionalOptions.size()];
        additionalOptions.toArray(additionalOptionsArr);

        if (additionalOptions.contains("--config"))
        {
            // no retry, use only users '--config' settings
            LvmCommands.createThin(
                extCmdFactory.create(),
                volumeGroup,
                vlmData.getThinPool(),
                lvId,
                vlmData.getExpectedSize(),
                null, // config is contained in additionalOptions
                additionalOptionsArr
            );

            LvmCommands.activateVolume(
                extCmdFactory.create(),
                volumeGroup,
                lvId,
                additionalOptions.get(additionalOptions.indexOf("--config") + 1)
            );
        }
        else
        {
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(volumeGroup),
                config -> LvmCommands.createThin(
                    extCmdFactory.create(),
                    volumeGroup,
                    vlmData.getThinPool(),
                    lvId,
                    vlmData.getExpectedSize(),
                    config,
                    additionalOptionsArr
                )
            );
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(volumeGroup),
                config -> LvmCommands.activateVolume(
                    extCmdFactory.create(),
                    volumeGroup,
                    lvId,
                    config
                )
            );
        }
    }

    @Override
    protected void deleteLvImpl(LvmData<Resource> lvmVlmData, String oldLvmId)
        throws StorageException, DatabaseException
    {
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(lvmVlmData.getVolumeGroup()),
            config -> LvmCommands.delete(
                extCmdFactory.create(),
                lvmVlmData.getVolumeGroup(),
                oldLvmId,
                config,
                LvmVolumeType.VOLUME
            )
        );
        lvmVlmData.setExists(false);
    }

    @Override
    protected boolean snapshotExists(LvmData<Snapshot> snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        String identifier = getFullQualifiedIdentifier(snapVlmRef);

        return infoListCache.get(identifier) != null;
    }

    @Override
    protected void createLvForBackupIfNeeded(LvmData<Snapshot> snapVlm)
        throws StorageException
    {
        try
        {
            List<String> additionalOptions = MkfsUtils.shellSplit(getLvcreateSnapshotOptions(snapVlm));
            String[] additionalOptionsArr = new String[additionalOptions.size()];
            additionalOptions.toArray(additionalOptionsArr);

            LvmData<Snapshot> prevSnapData = getPreviousSnapvlmData(
                snapVlm,
                snapVlm.getRscLayerObject().getAbsResource()
            );
            if (prevSnapData != null)
            {
                LvmUtils.execWithRetry(
                    extCmdFactory,
                    Collections.singleton(snapVlm.getVolumeGroup()),
                    config -> LvmCommands.createSnapshotThin(
                        extCmdFactory.create(),
                        snapVlm.getVolumeGroup(),
                        ((LvmThinData<Snapshot>) snapVlm).getThinPool(),
                        prevSnapData.getIdentifier(),
                        snapVlm.getIdentifier(),
                        config
                    )
                );
            }
            else
            {
                LvmUtils.execWithRetry(
                    extCmdFactory,
                    Collections.singleton(snapVlm.getVolumeGroup()),
                    config -> LvmCommands.createThin(
                        extCmdFactory.create(),
                        snapVlm.getVolumeGroup(),
                        ((LvmThinData<Snapshot>) snapVlm).getThinPool(),
                        snapVlm.getIdentifier(),
                        snapVlm.getUsableSize(),
                        config
                    )
                );
            }
        }
        catch (InvalidKeyException | AccessDeniedException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    protected void createSnapshot(LvmData<Resource> vlmDataRef, LvmData<Snapshot> snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        List<String> additionalOptions = MkfsUtils.shellSplit(getLvcreateSnapshotOptions(vlmDataRef));
        String[] additionalOptionsArr = new String[additionalOptions.size()];
        additionalOptions.toArray(additionalOptionsArr);

        LvmThinData<Resource> vlmData = (LvmThinData<Resource>) vlmDataRef;
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmData.getVolumeGroup()),
            config -> LvmCommands.createSnapshotThin(
                extCmdFactory.create(),
                vlmData.getVolumeGroup(),
                vlmData.getThinPool(),
                vlmData.getIdentifier(),
                getFullQualifiedIdentifier(snapVlmRef),
                config,
                additionalOptionsArr
            )
        );
    }

    @Override
    protected void deleteSnapshotImpl(LvmData<Snapshot> snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(snapVlm.getVolumeGroup()),
            config -> LvmCommands.delete(
                extCmdFactory.create(),
                getVolumeGroup(snapVlm.getStorPool()),
                asSnapLvIdentifier(snapVlm),
                config,
                LvmVolumeType.SNAPSHOT
            )
        );
        snapVlm.setExists(false);
    }

    @Override
    protected void restoreSnapshot(String sourceLvId, String sourceSnapName, LvmData<Resource> vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        String storageName = vlmData.getVolumeGroup();
        String targetId = asLvIdentifier(vlmData);
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmData.getVolumeGroup()),
            config -> LvmCommands.restoreFromSnapshot(
                extCmdFactory.create(),
                sourceLvId + "_" + sourceSnapName,
                storageName,
                targetId,
                config
            )
        );
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmData.getVolumeGroup()),
            config -> LvmCommands.activateVolume(
                extCmdFactory.create(),
                storageName,
                targetId,
                config
            )
        );
    }

    @Override
    protected void rollbackImpl(LvmData<Resource> lvmVlmData, String rollbackTargetSnapshotName)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        LvmThinData<Resource> vlmData = (LvmThinData<Resource>) lvmVlmData;

        String volumeGroup = vlmData.getVolumeGroup();
        String thinPool = vlmData.getThinPool();
        String targetLvId = asLvIdentifier(vlmData);
        String snapshotId = asSnapLvIdentifierRaw(
            vlmData.getStorPool().getName().displayValue,
            vlmData.getRscLayerObject().getResourceName().displayValue,
            vlmData.getRscLayerObject().getResourceNameSuffix(),
            rollbackTargetSnapshotName,
            vlmData.getVlmNr().value
        );
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmData.getVolumeGroup()),
            config -> LvmCommands.deactivateVolume(
                extCmdFactory.create(),
                volumeGroup,
                targetLvId,
                config
            )
        );

        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmData.getVolumeGroup()),
            config -> LvmCommands.rollbackToSnapshot(
                extCmdFactory.create(),
                volumeGroup,
                snapshotId,
                config
            )
        );

        // --merge removes the snapshot.
        // For consistency with other backends, we wish to keep the snapshot.
        // Hence we create it again here.
        // The layers above have been stopped, so the content should be identical to the original snapshot.

        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmData.getVolumeGroup()),
            config -> LvmCommands.createSnapshotThin(
                extCmdFactory.create(),
                volumeGroup,
                thinPool,
                targetLvId,
                snapshotId,
                config
            )
        );

        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmData.getVolumeGroup()),
            config -> LvmCommands.activateVolume(
                extCmdFactory.create(),
                volumeGroup,
                targetLvId,
                config
            )
        );
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPoolInfo storPool) throws StorageException, AccessDeniedException
    {
        String vgForLvs = getVolumeGroupForLvs(storPool);
        String thinPool = getThinPool(storPool);
        Long capacity = LvmUtils.getThinTotalSize(
            extCmdFactory,
            Collections.singleton(vgForLvs)
        ).get(thinPool);
        if (capacity == null)
        {
            throw new StorageException("Thin pool \'" + thinPool + "\' does not exist.");
        }

        Long freeSpace = LvmUtils.getThinFreeSize(
            extCmdFactory,
            Collections.singleton(vgForLvs)
        ).get(thinPool);
        if (freeSpace == null)
        {
            throw new StorageException("Thin pool \'" + thinPool + "\' does not exist.");
        }
        return SpaceInfo.buildOrThrowOnError(capacity, freeSpace, storPool);
    }

    @Override
    protected String getSnapshotShippingReceivingCommandImpl(LvmData<Snapshot> snapVlmDataRef) throws StorageException
    {
        return "thin_recv " + snapVlmDataRef.getVolumeGroup() + "/" + asSnapLvIdentifier(snapVlmDataRef);
    }

    @Override
    protected String getSnapshotShippingSendingCommandImpl(
        LvmData<Snapshot> lastSnapVlmDataRef,
        LvmData<Snapshot> curSnapVlmDataRef
    )
        throws StorageException
    {
        StringBuilder sb = new StringBuilder("thin_send ");
        if (lastSnapVlmDataRef != null)
        {
            sb.append(lastSnapVlmDataRef.getVolumeGroup()).append("/").append(lastSnapVlmDataRef.getIdentifier())
                .append(" ");
        }
        sb.append(curSnapVlmDataRef.getVolumeGroup()).append("/").append(curSnapVlmDataRef.getIdentifier());
        return sb.toString();
    }

    @Override
    protected void finishShipReceiving(LvmData<Resource> vlmDataRef, LvmData<Snapshot> snapVlmRef)
        throws StorageException, DatabaseException, AccessDeniedException
    {
        String vlmDataId = asLvIdentifier(vlmDataRef);
        deleteLvImpl(vlmDataRef, vlmDataId); // delete calls "lvmVlmData.setExists(false);" - we have to undo this
        LvmCommands.rename(
            extCmdFactory.create(),
            vlmDataRef.getVolumeGroup(),
            asSnapLvIdentifier(snapVlmRef),
            vlmDataId,
            null
        );
        vlmDataRef.setExists(true);

        // for keeping the same behavior as zfsProvider, we want to "keep" the snapshot. #
        createSnapshot(vlmDataRef, snapVlmRef);
    }

    private String getVolumeGroupForLvs(StorPoolInfo storPool) throws StorageException
    {
        String volumeGroup;
        String thinPool;
        try
        {
            volumeGroup = getVolumeGroup(storPool);
            if (volumeGroup == null)
            {
                throw new StorageException("Unset volume group for " + storPool);
            }
            thinPool = getThinPool(storPool);
            if (thinPool == null)
            {
                throw new StorageException("Unset thin pool for " + storPool);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return volumeGroup + File.separator + thinPool;
    }

    private String getThinPool(StorPoolInfo storPool) throws AccessDeniedException
    {
        String thinPool;
        try
        {
            thinPool = storPool.getReadOnlyProps(storDriverAccCtx)
                .getProp(StorageConstants.CONFIG_LVM_THIN_POOL_KEY, StorageConstants.NAMESPACE_STOR_DRIVER);
            if (!thinPool.contains("/"))
            {
                throw new ImplementationError(
                    String.format("Property '%s' doesn't contain a proper thinpool specifier: %s",
                        StorageConstants.CONFIG_LVM_THIN_POOL_KEY, thinPool));
            }

            thinPool = thinPool.split("/")[1];
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded key exception", exc);
        }
        return thinPool;
    }

    @Override
    protected void createLvWithCopyImpl(
        LvmData<Resource> lvmVlmData,
        Resource srcRsc)
        throws StorageException, AccessDeniedException
    {
        final LvmThinData<Resource> vlmData = (LvmThinData<Resource>) lvmVlmData;
        final LvmThinData<Resource> srcVlmData = (LvmThinData<Resource>) getVlmDataFromResource(
            srcRsc, vlmData.getRscLayerObject().getResourceNameSuffix(), vlmData.getVlmNr());

        final String srcId = asLvIdentifier(srcVlmData);
        final String srcFullSnapshotName = getCloneSnapshotNameFull(srcVlmData, vlmData, "_");
        final String dstId = asLvIdentifier(vlmData);

        List<String> additionalOptions = MkfsUtils.shellSplit(getLvcreateSnapshotOptions(lvmVlmData));
        String[] additionalOptionsArr = new String[additionalOptions.size()];
        additionalOptions.toArray(additionalOptionsArr);

        if (!infoListCache.containsKey(srcVlmData.getVolumeGroup() + "/" + srcFullSnapshotName))
        {
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(srcVlmData.getVolumeGroup()),
                config -> LvmCommands.createSnapshotThin(
                    extCmdFactory.create(),
                    srcVlmData.getVolumeGroup(),
                    srcVlmData.getThinPool(),
                    srcId,
                    srcFullSnapshotName,
                    config,
                    additionalOptionsArr
                )
            );

            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(srcVlmData.getVolumeGroup()),
                config -> LvmCommands.addTag(
                    extCmdFactory.create(),
                    srcVlmData.getVolumeGroup(),
                    srcFullSnapshotName,
                    LvmCommands.LVM_TAG_CLONE_SNAPSHOT,
                    config
                )
            );

            // restore
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmData.getVolumeGroup()),
                config -> LvmCommands.restoreFromSnapshot(
                    extCmdFactory.create(),
                    srcFullSnapshotName,
                    srcVlmData.getVolumeGroup(),
                    dstId,
                    config,
                    additionalOptionsArr
                )
            );

            // activate
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmData.getVolumeGroup()),
                config -> LvmCommands.activateVolume(
                    extCmdFactory.create(),
                    vlmData.getVolumeGroup(),
                    dstId,
                    config
                )
            );
        }
        else
        {
            errorReporter.logInfo("Clone base snapshot %s already found, reusing.", srcFullSnapshotName);
        }

        cloneService.startClone(
            srcVlmData,
            vlmData,
            this);
    }

    @Override
    public String[] getCloneCommand(CloneService.CloneInfo cloneInfo) {
        // LVM_THIN doesn't have a long run operation, but it is vital that the device manager runs
        // through before getting updated flags, so keep the clone daemon a bit busy
        return new String[] {"sleep", "1"};
    }

    @Override
    public LocalPropsChangePojo checkConfig(StorPoolInfo storPool) throws StorageException, AccessDeniedException
    {
        LocalPropsChangePojo ret = new LocalPropsChangePojo();

        ReadOnlyProps props = DeviceLayerUtils.getNamespaceStorDriver(
            storPool.getReadOnlyProps(storDriverAccCtx)
        );
        StorageConfigReader.checkThinPoolEntry(extCmdFactory, props);
        StorageConfigReader.checkToleranceFactor(props);

        String vlmGrp = getVolumeGroup(storPool);
        String thinPool = getThinPool(storPool);
        HashMap<String, LvsInfo> lvsInfo = LvmUtils.getLvsInfo(extCmdFactory, Collections.singleton(vlmGrp));
        LvsInfo thinPoolInfo = lvsInfo.get(vlmGrp + File.separator + thinPool);

        if (!thinPoolInfo.attributes.contains("z"))
        {
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmGrp),
                config -> LvmCommands.activateZero(extCmdFactory, vlmGrp, thinPool, config)
            );
        }

        ret.changeStorPoolProp(
            storPool,
            StorageConstants.NAMESPACE_INTERNAL + StorageConstants.KEY_INT_THIN_POOL_GRANULARITY,
            Long.toString(thinPoolInfo.chunkSizeInKib)
        );
        ret.changeStorPoolProp(
            storPool,
            StorageConstants.NAMESPACE_INTERNAL + StorageConstants.KEY_INT_THIN_POOL_METADATA_PERCENT,
            thinPoolInfo.metaDataPercentStr
        );

        super.checkExtentSize(storPool, ret);

        return ret;
    }

    /**
     * Instead of overriding getAllocatedSize() method (which is also used for setting usable size by the
     * AbsStorageProvider), we simply store in our LvmThinData the actual allocated size
     */
    @Override
    protected void setAllocatedSize(LvmData<Resource> vlmDataRef, long allocatedSize) throws DatabaseException
    {
        LvmThinData<Resource> lvmThinData = (LvmThinData<Resource>) vlmDataRef;
        super.setAllocatedSize(vlmDataRef, (long) (allocatedSize * lvmThinData.getDataPercent()));
    }
}
