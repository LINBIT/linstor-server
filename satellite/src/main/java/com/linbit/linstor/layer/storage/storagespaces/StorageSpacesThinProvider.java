package com.linbit.linstor.layer.storage.storagespaces;

import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.storagespaces.StorageSpacesData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class StorageSpacesThinProvider extends StorageSpacesProvider
{
    @Inject
    public StorageSpacesThinProvider(AbsStorageProviderInit superInitRef)
    {
        super(superInitRef, "STORAGE_SPACES_THIN", DeviceProviderKind.STORAGE_SPACES_THIN);
    }

    @Override
    protected void createLvImpl(StorageSpacesData<Resource> vlmData)
        throws StorageException
    {
        super.createLvImpl(vlmData);
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPoolInfo storPoolRef) throws StorageException
    {
        SpaceInfo info = super.getSpaceInfo(storPoolRef);

        /* We have a minimum extent size of 256MiB, so
         * it is very likely that thin disks take as
         * much data as thick disks.
         */

        return info;
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.STORAGE_SPACES_THIN;
    }
}
