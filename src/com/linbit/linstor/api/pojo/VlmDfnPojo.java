package com.linbit.linstor.api.pojo;

import com.linbit.linstor.VolumeDefinition;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class VlmDfnPojo implements VolumeDefinition.VlmDfnApi
{
    private final UUID uuid;
    private final Integer volumeNr;
    private final Integer minorNr;
    private final long size;
    private final long flags;
    private final Map<String, String> props;

    public VlmDfnPojo(
        final UUID uuidRef,
        final Integer volumeNrRef,
        final Integer minorNrRef,
        final long sizeRef,
        final long flagsRef,
        Map<String, String> propsRef
    )
    {
        uuid = uuidRef;
        volumeNr = volumeNrRef;
        minorNr = minorNrRef;
        size = sizeRef;
        flags = flagsRef;
        props = propsRef;
    }

    @Override
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public Integer getVolumeNr()
    {
        return volumeNr;
    }

    @Override
    public Integer getMinorNr()
    {
        return minorNr;
    }

    @Override
    public long getSize()
    {
        return size;
    }

    @Override
    public long getFlags()
    {
        return flags;
    }

    @Override
    public Map<String, String> getProps()
    {
        return props;
    }

}
