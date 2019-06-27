package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
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
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Singleton
public class LvmThinProvider extends LvmProvider
{
    private static final String ID_SNAP_DELIMITER = "_";
    public static final String FORMAT_SNAP_VLM_TO_LVM_ID = FORMAT_RSC_TO_LVM_ID + "_%s";

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
    protected void updateInfo(LvmData vlmDataRef, LvsInfo infoRef)
        throws AccessDeniedException, SQLException, StorageException
    {
        super.updateInfo(vlmDataRef, infoRef);
        LvmThinData lvmThinData = (LvmThinData) vlmDataRef;
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

    private List<String> asFullQualifiedLvIdentifierList(String rscNameSuffix, SnapshotVolume snapVlm)
        throws AccessDeniedException
    {
        List<String> fqLvIdList = new ArrayList<>();

        StorPool storPool = snapVlm.getStorPool(storDriverAccCtx);
        fqLvIdList.add(
            getVolumeGroup(storPool) + File.separator +
            String.format(
                FORMAT_SNAP_VLM_TO_LVM_ID,
                snapVlm.getResourceName().displayValue,
                rscNameSuffix,
                snapVlm.getVolumeNumber().value,
                snapVlm.getSnapshotName().displayValue
            )
        );

        return fqLvIdList;
    }

    @Override
    protected void createLvImpl(LvmData lvmVlmData) throws StorageException, AccessDeniedException
    {
        LvmThinData vlmData = (LvmThinData) lvmVlmData;
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
    protected void deleteLvImpl(LvmData lvmVlmData, String oldLvmId) throws StorageException, SQLException
    {
        LvmCommands.delete(
            extCmdFactory.create(),
            lvmVlmData.getVolumeGroup(),
            oldLvmId
        );
        lvmVlmData.setExists(false);
    }

    @Override
    protected boolean snapshotExists(SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        // FIXME: RAID: rscNameSuffix

        boolean oneExists = false;
        boolean allExsits = true;
        List<String> identifierList = asFullQualifiedLvIdentifierList("", snapVlm);

        for (String snapLvId : identifierList)
        {
            if (infoListCache.get(snapLvId) != null)
            {
                oneExists = true;
            }
            else
            {
                allExsits = false;
            }
        }
        if (oneExists && !allExsits)
        {
            // FIXME: what the heck should we do now?
            errorReporter.logError("Some, but not all LVs of snapshot " + snapVlm + " exist.");
        }
        return allExsits;
    }

    @Override
    protected void createSnapshot(LvmData lvmVlmData, SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        LvmThinData vlmData = (LvmThinData) lvmVlmData;
        String rscNameSuffix = vlmData.getRscLayerObject().getResourceNameSuffix();
        String snapshotIdentifier = getSnapshotIdentifier(rscNameSuffix, snapVlm);
        LvmCommands.createSnapshotThin(
            extCmdFactory.create(),
            vlmData.getVolumeGroup(),
            vlmData.getThinPool(),
            asLvIdentifier(
                rscNameSuffix,
                snapVlm.getResourceDefinition().getVolumeDfn(
                    storDriverAccCtx,
                    snapVlm.getVolumeNumber()
                )
            ),
            snapshotIdentifier
        );
    }

    @Override
    protected void deleteSnapshot(String rscNameSuffix, SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        LvmCommands.delete(
            extCmdFactory.create(),
            getVolumeGroup(snapVlm.getStorPool(storDriverAccCtx)),
            getSnapshotIdentifier(rscNameSuffix, snapVlm)
        );
    }

    @Override
    protected void restoreSnapshot(String sourceLvId, String sourceSnapName, LvmData vlmData)
        throws StorageException, AccessDeniedException, SQLException
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
    protected void rollbackImpl(LvmData lvmVlmData, String rollbackTargetSnapshotName)
        throws StorageException, AccessDeniedException, SQLException
    {
        LvmThinData vlmData = (LvmThinData) lvmVlmData;

        String volumeGroup = vlmData.getVolumeGroup();
        String thinPool = vlmData.getThinPool();
        String baseId = asLvIdentifier(vlmData);
        String snapshotId = getSnapshotIdentifier(baseId, rollbackTargetSnapshotName);

        LvmCommands.deactivateVolume(
            extCmdFactory.create(),
            volumeGroup,
            baseId
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
            baseId,
            snapshotId
        );

        LvmCommands.activateVolume(
            extCmdFactory.create(),
            volumeGroup,
            baseId
        );
    }

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String vgForLvs = getVolumeGroupForLvs(storPool);
        String thinPool = getThinPool(storPool);
        Long ret = LvmUtils.getThinTotalSize(
            extCmdFactory.create(),
            Collections.singleton(vgForLvs)
        ).get(thinPool);
        if (ret == null)
        {
            throw new StorageException("Thin pool \'" + thinPool + "\' does not exist.");
        }
        return ret;
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String vgForLvs = getVolumeGroupForLvs(storPool);
        String thinPool = getThinPool(storPool);
        Long ret = LvmUtils.getThinFreeSize(
            extCmdFactory.create(),
            Collections.singleton(vgForLvs)
        ).get(thinPool);
        if (ret == null)
        {
            throw new StorageException("Thin pool \'" + thinPool + "\' does not exist.");
        }
        return ret;
    }

    @Override
    protected long getAllocatedSize(LvmData vlmDataRef) throws StorageException
    {
        LvmThinData lvmThinData = (LvmThinData) vlmDataRef;
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

    /**
     * @param snapVlm
     * @return "rscName_vlmNr" + "_" + "snapshotName"
     * @throws AccessDeniedException
     */
    private String getSnapshotIdentifier(String rscNameSuffix, SnapshotVolume snapVlm)
    {
        return getSnapshotIdentifier(
            asLvIdentifier(rscNameSuffix, snapVlm.getSnapshotVolumeDefinition()),
            snapVlm.getSnapshotName().displayValue
        );
    }

    private String getSnapshotIdentifier(String baseId, String snapshotName)
    {
        return baseId + ID_SNAP_DELIMITER + snapshotName;
    }
}
