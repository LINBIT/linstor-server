package com.linbit.linstor.layer.storage.storagespaces;

import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.storagespaces.StorageSpacesData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class StorageSpacesThinProvider extends StorageSpacesProvider
{
        /* Those are in bytes. */
        /* One thin volume uses that much bytes at least: */
    private final Long MINIMAL_THIN_SIZE_ON_DISK = 256L*1024L*1024L;
        /* One thin volume (the usable partition) can be that much bytes
           maximum: 4EiB - 17MiB (for the partition overhead): */
    private final Long MAXIMAL_THIN_SIZE = 1024L*1024L*1024L*1024L*1024L*1024L*4L - 17L*1024L*1024L;

    @Inject
    public StorageSpacesThinProvider(AbsStorageProviderInit superInitRef)
    {
        super(superInitRef, "STORAGE_SPACES_THIN", DeviceProviderKind.STORAGE_SPACES_THIN);
    }

    @Override
    protected void createLvImpl(StorageSpacesData<Resource> vlmData)
        throws StorageException, AccessDeniedException
    {
        super.createLvImpl(vlmData);
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPoolInfo storPoolRef) throws AccessDeniedException, StorageException
    {
        SpaceInfo info = super.getSpaceInfo(storPoolRef);

        if (info.freeCapacity*1024 >= MINIMAL_THIN_SIZE_ON_DISK)
        {
            info.freeCapacity = MAXIMAL_THIN_SIZE / 1024;
        }
        return info;
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.STORAGE_SPACES_THIN;
    }
}
