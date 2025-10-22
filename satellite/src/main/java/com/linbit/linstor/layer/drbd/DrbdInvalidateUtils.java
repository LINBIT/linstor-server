package com.linbit.linstor.layer.drbd;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.layer.drbd.utils.DrbdAdm;
import com.linbit.linstor.layer.drbd.utils.MdSuperblockBuffer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;

/**
 * This dedicated class is external of the core {@link DrbdLayer} class simply because the {@link DrbdLayer} itself
 * will usually not invalidate its own metadata (apart from restoring snapshots from backups) under ordinary
 * circumstances.
 * The intention of this class is to be called from other layers that have valid reasons to tell the
 * {@link DrbdLayer} that the metadata might need to be recreated (i.e. when the backing disk is (re-) created).
 *
 * For this class, we do not care if the DRBD resource is up or not. We are deliberately _not_ calling
 * "drbdadm invalidate $rsc" since in a few tests it had issues (see DRBD GL issue 80).
 * Instead we are wiping the superblock of the external metadata and mark the {@link DrbdVlmData} that it's metadata
 * needs to be checked so that the next time {@link DrbdLayer} processes the resource it should no longer find valid
 * (external) metadata and therefore create entirely new one.
 * Creating new metadata is also preferred over "invalidate" since an invalidation would cause a DRBD FullSync, while
 * new metadata would still allow day0-based partial sync.
 */
@Singleton
public class DrbdInvalidateUtils
{
    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;
    private final DrbdAdm drbdUtils;

    @Inject
    public DrbdInvalidateUtils(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        DrbdAdm drbdUtilsRef
    )
    {
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
        drbdUtils = drbdUtilsRef;
    }

    public void invalidate(@Nullable AbsRscLayerObject<Resource> rscData) throws StorageException, AccessDeniedException
    {
        if (rscData != null)
        {
            if (rscData.getLayerKind() == DeviceLayerKind.DRBD)
            {
                DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) rscData;
                for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
                {
                    if (hasSurvivingExternalMetadata(drbdVlmData))
                    {
                        invalidateImpl(drbdVlmData);
                    }
                }
            }
            else
            {
                invalidate(rscData.getParent());
            }
        }
    }

    public void invalidate(@Nullable VlmProviderObject<Resource> vlmData) throws StorageException, AccessDeniedException
    {
        if (vlmData != null)
        {
            if (vlmData.getLayerKind() == DeviceLayerKind.DRBD)
            {
                DrbdVlmData<Resource> drbdVlmData = (DrbdVlmData<Resource>) vlmData;
                if (hasSurvivingExternalMetadata(drbdVlmData))
                {
                    invalidateImpl(drbdVlmData);
                }
            }
            else
            {
                @Nullable AbsRscLayerObject<Resource> parentRscData = vlmData.getRscLayerObject().getParent();
                if (parentRscData != null)
                {
                    @Nullable VlmProviderObject<Resource> parentVlmData = parentRscData.getVlmProviderObject(
                        vlmData.getVlmNr()
                    );
                    if (parentVlmData != null)
                    {
                        invalidate(parentVlmData);
                    }
                    else
                    {
                        invalidate(parentRscData);
                    }
                }
            }
        }
    }

    private void invalidateImpl(DrbdVlmData<Resource> drbdVlmDataRef) throws StorageException
    {
        try
        {
            errorReporter.logInfo("Wiping superblock of external metadata of vlm: %s", drbdVlmDataRef.getIdentifier());
            MdSuperblockBuffer.wipe(extCmdFactory, drbdVlmDataRef.getMetaDiskPath(), true);
            drbdVlmDataRef.setCheckMetaData(true);
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "Failed to wipe DRBD ext MD for volume: " + drbdVlmDataRef.getIdentifier(),
                exc
            );
        }
    }

    private boolean hasSurvivingExternalMetadata(DrbdVlmData<Resource> drbdVlmDataRef) throws StorageException
    {
        boolean ret = false;
        final VolumeNumber vlmNr = drbdVlmDataRef.getVlmNr();
        final @Nullable VlmProviderObject<Resource> metaChild = drbdVlmDataRef.getChildBySuffix(
            RscLayerSuffixes.SUFFIX_DRBD_META
        );
        if (metaChild != null)
        {
            for (AbsRscLayerObject<Resource> storRscData : LayerRscUtils.getRscDataByLayer(
                metaChild.getRscLayerObject(),
                DeviceLayerKind.STORAGE
            ))
            {
                @Nullable VlmProviderObject<Resource> storVlmData = storRscData.getVlmProviderObject(vlmNr);
                @Nullable String metaDiskPath = drbdVlmDataRef.getMetaDiskPath();
                try
                {
                    if (storVlmData != null &&
                        storVlmData.exists() &&
                        metaDiskPath != null &&
                        drbdUtils.hasMetaData(
                            metaDiskPath,
                            drbdVlmDataRef.getVlmDfnLayerObject().getMinorNr().value,
                            "flex-external"
                        ))
                    {
                        ret = true;
                    }
                }
                catch (ExtCmdFailedException exc)
                {
                    throw new StorageException("Failed to figure out if valid external metadata exist", exc);
                }
            }
        }
        return ret;
    }
}
