package com.linbit.linstor.api.pojo;

import com.linbit.linstor.Volume;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.utils.Pair;

import java.util.List;
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
    private final String devicePath;
    private final int vlmNr;
    private final long vlmFlags;
    private final Map<String, String> vlmProps;
    private final String storDriverName;
    private final UUID storPoolDfnUuid;
    private Map<String, String> storPoolDfnProps;
    private Map<String, String> storPoolProps;
    private final Optional<Long> allocated;
    private Optional<Long> usableSize;
    private final List<Pair<String, VlmLayerDataApi>> layerData;

    public VlmPojo(
        final String storagePoolNameRef,
        final UUID storagePoolUuidRef,
        final UUID vlmDfnUuidRef,
        final UUID vlmUuidRef,
        final String devicePathRef,
        final int vlmNrRef,
        final long vlmFlagsRef,
        final Map<String, String> vlmPropsRef,
        final String storDriverNameRef,
        final UUID storPoolDfnUuidRef,
        final Map<String, String> storPoolDfnPropsRef,
        final Map<String, String> storPoolPropsRef,
        final Optional<Long> allocatedRef,
        final Optional<Long> usableSizeRef,
        final List<Pair<String, VlmLayerDataApi>> layerDataRef
    )
    {
        storagePoolName = storagePoolNameRef;
        storagePoolUuid = storagePoolUuidRef;
        vlmDfnUuid = vlmDfnUuidRef;
        vlmUuid = vlmUuidRef;
        devicePath = devicePathRef;
        vlmNr = vlmNrRef;
        vlmFlags = vlmFlagsRef;
        vlmProps = vlmPropsRef;
        storDriverName = storDriverNameRef;
        storPoolDfnUuid = storPoolDfnUuidRef;
        storPoolDfnProps = storPoolDfnPropsRef;
        storPoolProps = storPoolPropsRef;
        allocated = allocatedRef;
        usableSize = usableSizeRef;
        layerData = layerDataRef;
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

    @Override
    public List<Pair<String, VlmLayerDataApi>> getVlmLayerData()
    {
        return layerData;
    }
}
