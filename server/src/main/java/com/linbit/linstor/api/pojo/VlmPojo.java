package com.linbit.linstor.api.pojo;

import com.linbit.linstor.Volume;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class VlmPojo implements Volume.VlmApi
{
    private final String storagePoolName;
    private final UUID storagePoolUuid;
    private final UUID vlmDfnUuid;
    private final UUID vlmUuid;
    private final String blockDevice;
    private final String metaDisk;
    private final String devicePath;
    private final int vlmNr;
    private final int vlmMinorNr;
    private final long vlmFlags;
    private final Map<String, String> vlmProps;
    private final String storDriverName;
    private final UUID storPoolDfnUuid;
    private Map<String, String> storPoolDfnProps;
    private Map<String, String> storPoolProps;
    private final Optional<Long> allocated;
    private Optional<Long> usableSize;

    public VlmPojo(
        final String storagePoolNameRef,
        final UUID storagePoolUuidRef,
        final UUID vlmDfnUuidRef,
        final UUID vlmUuidRef,
        final String blockDeviceRef,
        final String metaDiskRef,
        final String devicePathRef,
        final int vlmNrRef,
        final int vlmMinorNrRef,
        final long vlmFlagsRef,
        final Map<String, String> vlmPropsRef,
        final String storDriverNameRef,
        final UUID storPoolDfnUuidRef,
        final Map<String, String> storPoolDfnPropsRef,
        final Map<String, String> storPoolPropsRef,
        final Optional<Long> allocatedRef,
        final Optional<Long> usableSizeRef
    )
    {
        storagePoolName = storagePoolNameRef;
        storagePoolUuid = storagePoolUuidRef;
        vlmDfnUuid = vlmDfnUuidRef;
        vlmUuid = vlmUuidRef;
        blockDevice = blockDeviceRef;
        metaDisk = metaDiskRef;
        devicePath = devicePathRef;
        vlmNr = vlmNrRef;
        vlmMinorNr = vlmMinorNrRef;
        vlmFlags = vlmFlagsRef;
        vlmProps = vlmPropsRef;
        storDriverName = storDriverNameRef;
        storPoolDfnUuid = storPoolDfnUuidRef;
        storPoolDfnProps = storPoolDfnPropsRef;
        storPoolProps = storPoolPropsRef;
        allocated = allocatedRef;
        usableSize = usableSizeRef;
    }

    @Override
    public String getStorPoolName()
    {
        return storagePoolName;
    }

    @Override
    public UUID getStorPoolUuid()
    {
        return storagePoolUuid;
    }

    @Override
    public UUID getVlmDfnUuid()
    {
        return vlmDfnUuid;
    }

    @Override
    public UUID getVlmUuid()
    {
        return vlmUuid;
    }

    @Override
    public String getBlockDevice()
    {
        return blockDevice;
    }

    @Override
    public String getMetaDisk()
    {
        return metaDisk;
    }

    @Override
    public String getDevicePath()
    {
        return devicePath;
    }

    @Override
    public int getVlmNr()
    {
        return vlmNr;
    }

    @Override
    public int getVlmMinorNr()
    {
        return vlmMinorNr;
    }

    @Override
    public long getFlags()
    {
        return vlmFlags;
    }

    @Override
    public Map<String, String> getVlmProps()
    {
        return vlmProps;
    }

    @Override
    public String getStorDriverSimpleClassName()
    {
        return storDriverName;
    }

    @Override
    public UUID getStorPoolDfnUuid()
    {
        return storPoolDfnUuid;
    }

    @Override
    public Map<String, String> getStorPoolDfnProps()
    {
        return storPoolDfnProps;
    }

    @Override
    public Map<String, String> getStorPoolProps()
    {
        return storPoolProps;
    }

    @Override
    public Optional<Long> getAllocatedSize()
    {
        return allocated;
    }

    @Override
    public Optional<Long> getUsableSize()
    {
        return usableSize;
    }
}
