package com.linbit.linstor.storage;

public class StorageSpacesInfo extends VolumeInfo
{
    private String storagePoolFriendlyName;
    private long allocatedSize;

    public StorageSpacesInfo(final long size, final long allocatedSizeRef, final String identifier, final String path, final String storagePoolFriendlyNameRef)
    {
        super(size, identifier, path);
        storagePoolFriendlyName = storagePoolFriendlyNameRef;
        allocatedSize = allocatedSizeRef;
    }

    public String getStoragePoolFriendlyName()
    {
        return storagePoolFriendlyName;
    }

    public long getAllocatedSize()
    {
        return allocatedSize;
    }
}
