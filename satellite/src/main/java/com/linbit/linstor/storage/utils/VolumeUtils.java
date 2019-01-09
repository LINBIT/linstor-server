package com.linbit.linstor.storage.utils;

import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;

import java.util.ArrayList;
import java.util.List;

public class VolumeUtils
{
    public static VlmProviderObject getBackingVolume(VlmProviderObject vlm)
        throws StorageException
    {
        List<VlmProviderObject> backingVlms = getBackingVolumes(vlm);

        if (backingVlms.size() > 1)
        {
            throw new StorageException("Only one backing volume expected. " + vlm);
        }

        return backingVlms.isEmpty() ? null : backingVlms.get(0);
    }

    public static List<VlmProviderObject> getBackingVolumes(VlmProviderObject vlm)
    {
        VolumeNumber volumeNumber = vlm.getVlmNr();
        List<RscLayerObject> childResources = vlm.getRscLayerObject().getChildren();

        List<VlmProviderObject> backingVolumes = new ArrayList<>();
        for (RscLayerObject backingResource : childResources)
        {
            backingVolumes.add(backingResource.getVlmProviderObject(volumeNumber));
        }
        return backingVolumes;
    }

    public static boolean isVolumeThinlyBacked(VlmProviderObject vlmData)
        throws StorageException
    {
        VlmProviderObject backingVlm = vlmData;
        VlmProviderObject tmp = backingVlm;
        while (tmp != null)
        {
            tmp = getBackingVolume(backingVlm);
            if (tmp == null)
            {
                break;
            }
            backingVlm = tmp;
        }
        return backingVlm.getProviderKind().usesThinProvisioning();
    }
}
