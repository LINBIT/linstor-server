package com.linbit.linstor.storage.utils;

import com.linbit.linstor.Resource;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;

import java.util.ArrayList;
import java.util.List;

public class VolumeUtils
{
    public static Volume getBackingVolume(AccessContext accCtx, Volume vlm)
        throws StorageException, AccessDeniedException
    {
        List<Volume> backingVlms = getBackingVolumes(accCtx, vlm);

        if (backingVlms.size() > 1)
        {
            throw new StorageException("Only one backing volume expected. " + vlm);
        }

        return backingVlms.isEmpty() ? null : backingVlms.get(0);
    }

    public static List<Volume> getBackingVolumes(AccessContext accCtx, Volume vlm)
        throws AccessDeniedException
    {
        VolumeNumber volumeNumber = vlm.getVolumeDefinition().getVolumeNumber();
        List<Resource> childResources = vlm.getResource().getChildResources(accCtx);

        List<Volume> backingVolumes = new ArrayList<>();
        for (Resource backingResource : childResources)
        {
            backingVolumes.add(backingResource.getVolume(volumeNumber));
        }
        return backingVolumes;
    }

    public static boolean isVolumeThinlyBacked(AccessContext accCtx, Volume vlm)
        throws AccessDeniedException, StorageException
    {
        Volume backingVlm = vlm;
        Volume tmp = backingVlm;
        while (tmp != null)
        {
            tmp = getBackingVolume(accCtx, backingVlm);
            if (tmp == null)
            {
                break;
            }
            backingVlm = tmp;
        }

        return backingVlm.getStorPool(accCtx).getDriverKind().usesThinProvisioning();
    }
}
