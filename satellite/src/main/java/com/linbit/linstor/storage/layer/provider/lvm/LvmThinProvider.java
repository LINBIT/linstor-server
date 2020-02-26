package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.WipeHandler;
import com.linbit.linstor.storage.utils.LvmCommands;
import com.linbit.linstor.storage.utils.LvmUtils;
import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.util.Collections;

@Singleton
public class LvmThinProvider extends LvmProvider
{
    @Inject
    public LvmThinProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        @DeviceManagerContext AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider
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
            DeviceProviderKind.LVM_THIN
        );
    }

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
            if (!infoRef.attributes.contains("a"))
            {
                LvmCommands.activateVolume(
                    extCmdFactory.create(),
                    lvmThinData.getVolumeGroup(),
                    lvmThinData.getIdentifier()
                );
            }
        }
    }

    @Override
    protected void createLvImpl(LvmData<Resource> lvmVlmData) throws StorageException, AccessDeniedException
    {
        LvmThinData<Resource> vlmData = (LvmThinData<Resource>) lvmVlmData;
        String volumeGroup = vlmData.getVolumeGroup();
        String lvId = asLvIdentifier(vlmData);
        LvmCommands.createThin(
            extCmdFactory.create(),
            volumeGroup,
            vlmData.getThinPool(),
            lvId,
            vlmData.getExepectedSize()
        );
        LvmCommands.activateVolume(
            extCmdFactory.create(),
            volumeGroup,
            lvId
        );
    }

    @Override
    protected void deleteLvImpl(LvmData<Resource> lvmVlmData, String oldLvmId)
        throws StorageException, DatabaseException
    {
        LvmCommands.delete(
            extCmdFactory.create(),
            lvmVlmData.getVolumeGroup(),
            oldLvmId
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
    protected void createSnapshot(LvmData<Resource> vlmDataRef, LvmData<Snapshot> snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        LvmThinData<Resource> vlmData = (LvmThinData<Resource>) vlmDataRef;
        LvmCommands.createSnapshotThin(
            extCmdFactory.create(),
            vlmData.getVolumeGroup(),
            vlmData.getThinPool(),
            vlmData.getIdentifier(),
            getFullQualifiedIdentifier(snapVlmRef)
        );
    }

    @Override
    protected void deleteSnapshot(LvmData<Snapshot> snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        LvmCommands.delete(
            extCmdFactory.create(),
            getVolumeGroup(snapVlm.getStorPool()),
            asSnapLvIdentifier(snapVlm)
        );
        snapVlm.setExists(false);
    }

    @Override
    protected void restoreSnapshot(String sourceLvId, String sourceSnapName, LvmData<Resource> vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        String storageName = vlmData.getVolumeGroup();
        String targetId = asLvIdentifier(vlmData);
        LvmCommands.restoreFromSnapshot(
            extCmdFactory.create(),
            sourceLvId + "_" + sourceSnapName,
            storageName,
            targetId
        );
        LvmCommands.activateVolume(
            extCmdFactory.create(),
            storageName,
            targetId
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
            vlmData.getRscLayerObject().getResourceName().displayValue,
            vlmData.getRscLayerObject().getResourceNameSuffix(),
            rollbackTargetSnapshotName,
            vlmData.getVlmNr().value
        );
        LvmCommands.deactivateVolume(
            extCmdFactory.create(),
            volumeGroup,
            targetLvId
        );

        LvmCommands.rollbackToSnapshot(
            extCmdFactory.create(),
            volumeGroup,
            snapshotId
        );

        // --merge removes the snapshot.
        // For consistency with other backends, we wish to keep the snapshot.
        // Hence we create it again here.
        // The layers above have been stopped, so the content should be identical to the original snapshot.

        LvmCommands.createSnapshotThin(
            extCmdFactory.create(),
            volumeGroup,
            thinPool,
            targetLvId,
            snapshotId
        );

        LvmCommands.activateVolume(
            extCmdFactory.create(),
            volumeGroup,
            targetLvId
        );
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String vgForLvs = getVolumeGroupForLvs(storPool);
        String thinPool = getThinPool(storPool);
        Long capacity = LvmUtils.getThinTotalSize(
            extCmdFactory.create(),
            Collections.singleton(vgForLvs)
        ).get(thinPool);
        if (capacity == null)
        {
            throw new StorageException("Thin pool \'" + thinPool + "\' does not exist.");
        }

        Long freeSpace = LvmUtils.getThinFreeSize(
            extCmdFactory.create(),
            Collections.singleton(vgForLvs)
        ).get(thinPool);
        if (freeSpace == null)
        {
            throw new StorageException("Thin pool \'" + thinPool + "\' does not exist.");
        }
        return new SpaceInfo(capacity, freeSpace);
    }

    @Override
    protected long getAllocatedSize(LvmData<Resource> vlmDataRef) throws StorageException
    {
        LvmThinData<Resource> lvmThinData = (LvmThinData<Resource>) vlmDataRef;
        long allocatedSize = super.getAllocatedSize(vlmDataRef);
        return (long) (allocatedSize * lvmThinData.getDataPercent());
    }

    private String getVolumeGroupForLvs(StorPool storPool) throws StorageException
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

    private String getThinPool(StorPool storPool) throws AccessDeniedException
    {
        String thinPool;
        try
        {
            thinPool = storPool.getProps(storDriverAccCtx).getProp(
                StorageConstants.CONFIG_LVM_THIN_POOL_KEY,
                StorageConstants.NAMESPACE_STOR_DRIVER
            );
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded key exception", exc);
        }
        return thinPool;
    }
}
