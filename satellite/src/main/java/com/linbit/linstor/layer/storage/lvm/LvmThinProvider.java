package com.linbit.linstor.layer.storage.lvm;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyStorPool;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
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

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
        BackupShippingMgr backupShipMgrRef,
        FileSystemWatch fileSystemWatchRef
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
            backupShipMgrRef,
            fileSystemWatchRef
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
    protected void createLvImpl(LvmData<Resource> lvmVlmData) throws StorageException
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
        LvmUtils.recacheNextLvs();
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
        LvmUtils.recacheNextLvs();
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
            LvmUtils.recacheNextLvs();
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
        LvmUtils.recacheNextLvs();
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
        LvmUtils.recacheNextLvs();
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
        LvmUtils.recacheNextLvs();
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
        LvmUtils.recacheNextLvs();
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPoolInfo storPool) throws StorageException, AccessDeniedException
    {
        String vgName = getVolumeGroup(storPool);
        String thinPool = getThinPool(storPool);
        String vgWithThinPool = vgName + File.separator + thinPool;

        Map<String /* vg */, Map<String/* thinPool */, LvsInfo>> thinTotalSizeMap = LvmUtils.getLvsInfo(
            extCmdFactory,
            Collections.singleton(vgName)
        );
        Map<String /* vg */, Map<String/* thinPool */, Long>> thinFreeSizeMap = LvmUtils.getThinFreeSize(
            extCmdFactory,
            Collections.singleton(vgName)
        );

        @Nullable Map<String, LvsInfo> lvMap = thinTotalSizeMap.get(vgName);
        if (lvMap == null)
        {
            throw new StorageException("Volume group \'" + vgName + "\' does not exist.");
        }
        @Nullable LvsInfo lvsInfo = lvMap.get(thinPool);
        if (lvsInfo == null)
        {
            throw new StorageException("Thin pool \'" + vgWithThinPool + "\' does not exist.");
        }

        @Nullable Map<String, Long> thinMap = thinFreeSizeMap.get(vgName);
        if (thinMap == null)
        {
            throw new StorageException("Volume group \'" + vgName + "\' does not exist.");
        }
        @Nullable Long freeSpace = thinMap.get(thinPool);
        if (freeSpace == null)
        {
            throw new StorageException("Thin pool \'" + vgWithThinPool + "\' does not exist.");
        }
        return SpaceInfo.buildOrThrowOnError(lvsInfo.size, freeSpace, storPool);
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
        LvmUtils.recacheNextLvs();

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

    /**
     * Create a thin snapshot instead of supers thick snapshot
     * @param lvmVlmData
     * @param cloneRscName
     * @throws StorageException
     */
    @Override
    protected void createSnapshotForCloneImpl(
        LvmData<Resource> lvmVlmData,
        String cloneRscName)
        throws StorageException
    {
        final LvmThinData<Resource> vlmData = (LvmThinData<Resource>) lvmVlmData;

        final String srcId = asLvIdentifier(vlmData);
        final String srcFullSnapshotName = getCloneSnapshotNameFull(vlmData, cloneRscName, "_");

        List<String> additionalOptions = MkfsUtils.shellSplit(getLvcreateSnapshotOptions(lvmVlmData));
        String[] additionalOptionsArr = new String[additionalOptions.size()];
        additionalOptions.toArray(additionalOptionsArr);

        if (!infoListCache.containsKey(vlmData.getVolumeGroup() + "/" + srcFullSnapshotName))
        {
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmData.getVolumeGroup()),
                config -> LvmCommands.createSnapshotThin(
                    extCmdFactory.create(),
                    vlmData.getVolumeGroup(),
                    vlmData.getThinPool(),
                    srcId,
                    srcFullSnapshotName,
                    config,
                    additionalOptionsArr
                )
            );

            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmData.getVolumeGroup()),
                config -> LvmCommands.addTag(
                    extCmdFactory.create(),
                    vlmData.getVolumeGroup(),
                    srcFullSnapshotName,
                    LvmCommands.LVM_TAG_CLONE_SNAPSHOT,
                    config
                )
            );

            LvmUtils.recacheNextLvs();
        }
        else
        {
            errorReporter.logInfo("Clone base snapshot %s already found, reusing.", srcFullSnapshotName);
        }
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
        Map<String, Map<String, LvsInfo>> lvsInfo = LvmUtils.getLvsInfo(extCmdFactory, Collections.singleton(vlmGrp));

        @Nullable Map<String, LvsInfo> lvMap = lvsInfo.get(vlmGrp);
        if (lvMap == null)
        {
            throw new StorageException("Volume group '" + vlmGrp + "' does not exist");
        }
        @Nullable LvsInfo thinPoolInfo = lvMap.get(thinPool);
        if (thinPoolInfo == null)
        {
            throw new StorageException("Thin pool '" + vlmGrp + "/" + thinPool + "' does not exist");
        }

        if (!thinPoolInfo.attributes.contains("z"))
        {
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmGrp),
                config -> LvmCommands.activateZero(extCmdFactory, vlmGrp, thinPool, config)
            );
            LvmUtils.recacheNextLvs();
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

    @Override
    public Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException, AccessDeniedException
    {
        Map<ReadOnlyVlmProviderInfo, Long> ret = new HashMap<>();
        Set<String> lvmVolumeGroups = new HashSet<>();

        Map<String, List<ReadOnlyVlmProviderInfo>> roVlmProvInfoByLvmVlmGrp = new HashMap<>();
        for (ReadOnlyVlmProviderInfo roVlmProvInfo : vlmDataListRef)
        {
            ReadOnlyStorPool roStorPool = roVlmProvInfo.getReadOnlyStorPool();
            String vlmGrp = getVolumeGroup(roStorPool);
            roVlmProvInfoByLvmVlmGrp.computeIfAbsent(vlmGrp, ignored -> new ArrayList<>())
                .add(roVlmProvInfo);
            lvmVolumeGroups.add(vlmGrp);
        }

        Map<String/* lvm VG */, Map<String/* lvs id */, LvsInfo>> lvsInfoMap = LvmUtils.getLvsInfo(
            extCmdFactory,
            lvmVolumeGroups
        );

        for (Entry<String /* lvm VG */, List<ReadOnlyVlmProviderInfo>> entry : roVlmProvInfoByLvmVlmGrp.entrySet())
        {
            String lvmVlmGrp = entry.getKey();
            @Nullable Map<String/* lvs identifier */, LvsInfo> lvsInfoForCurrentVolumeGrp = lvsInfoMap.get(lvmVlmGrp);
            if (lvsInfoForCurrentVolumeGrp == null)
            {
                for (ReadOnlyVlmProviderInfo roVlmProvInfo : entry.getValue())
                {
                    ret.put(roVlmProvInfo, roVlmProvInfo.getOrigAllocatedSize());
                }
            }
            else
            {
                for (ReadOnlyVlmProviderInfo roVlmProvInfo : entry.getValue())
                {
                    @Nullable String identifier = roVlmProvInfo.getIdentifier();
                    if (identifier == null)
                    {
                        identifier = asLvIdentifier(
                            roVlmProvInfo.getReadOnlyStorPool().getName(),
                            roVlmProvInfo.getResourceName(),
                            roVlmProvInfo.getRscSuffix(),
                            roVlmProvInfo.getVlmNr()
                        );
                    }
                    @Nullable LvsInfo lvInfo = lvsInfoForCurrentVolumeGrp.get(identifier);
                    long allocatedSize;
                    if (lvInfo == null)
                    {
                        allocatedSize = roVlmProvInfo.getOrigAllocatedSize();
                    }
                    else
                    {
                        // lvInfo.size should already be in KiB
                        allocatedSize = (long) (lvInfo.size * (lvInfo.dataPercent / 100.0f));
                    }
                    ret.put(roVlmProvInfo, allocatedSize);
                }
            }
        }

        return ret;
    }
}
