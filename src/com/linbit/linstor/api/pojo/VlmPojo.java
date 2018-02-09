package com.linbit.linstor.api.pojo;

import com.linbit.linstor.Volume;
import java.util.Map;
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
    private final int vlmNr;
    private final int vlmMinorNr;
    private final long vlmFlags;
    private final Map<String, String> vlmProps;

    public VlmPojo(
        final String storagePoolNameRef,
        final UUID storagePoolUuidRef,
        final UUID vlmDfnUuidRef,
        final UUID vlmUuidRef,
        final String blockDeviceRef,
        final String metaDiskRef,
        final int vlmNrRef,
        final int vlmMinorNrRef,
        final long vlmFlagsRef,
        final Map<String, String> vlmPropsRef
    )
    {
        storagePoolName = storagePoolNameRef;
        storagePoolUuid = storagePoolUuidRef;
        vlmDfnUuid = vlmDfnUuidRef;
        vlmUuid = vlmUuidRef;
        blockDevice = blockDeviceRef;
        metaDisk = metaDiskRef;
        vlmNr = vlmNrRef;
        vlmMinorNr = vlmMinorNrRef;
        vlmFlags = vlmFlagsRef;
        vlmProps = vlmPropsRef;
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

}
