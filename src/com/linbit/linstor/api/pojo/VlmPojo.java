/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linbit.linstor.api.pojo;

import com.linbit.linstor.Volume;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class VlmPojo implements Volume.VlmApi {
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
        final String storagePoolName,
        final UUID storagePoolUuid,
        final UUID vlmDfnUuid,
        final UUID vlmUuid,
        final String blockDevice,
        final String metaDisk,
        final int vlmNr,
        final int vlmMinorNr,
        final long vlmFlags,
        final Map<String, String> vlmProps)
    {
        this.storagePoolName = storagePoolName;
        this.storagePoolUuid = storagePoolUuid;
        this.vlmDfnUuid = vlmDfnUuid;
        this.vlmUuid = vlmUuid;
        this.blockDevice = blockDevice;
        this.metaDisk = metaDisk;
        this.vlmNr = vlmNr;
        this.vlmMinorNr = vlmMinorNr;
        this.vlmFlags = vlmFlags;
        this.vlmProps = vlmProps;
    }

    @Override
    public String getStorPoolName() {
        return storagePoolName;
    }

    @Override
    public UUID getStorPoolUuid() {
        return storagePoolUuid;
    }

    @Override
    public UUID getVlmDfnUuid() {
        return vlmDfnUuid;
    }

    @Override
    public UUID getVlmUuid() {
        return vlmUuid;
    }

    @Override
    public String getBlockDevice() {
        return blockDevice;
    }

    @Override
    public String getMetaDisk() {
        return metaDisk;
    }

    @Override
    public int getVlmNr() {
        return vlmNr;
    }

    @Override
    public int getVlmMinorNr() {
        return vlmMinorNr;
    }

    @Override
    public long getFlags() {
        return vlmFlags;
    }

    @Override
    public Map<String, String> getVlmProps() {
        return vlmProps;
    }

}
