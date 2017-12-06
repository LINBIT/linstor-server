/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linbit.linstor.api.pojo;

import com.linbit.linstor.VolumeDefinition;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class VlmDfnPojo implements VolumeDefinition.VlmDfnApi {
    private final UUID uuid;
    private final Integer volumeNr;
    private final Integer minorNr;
    private final long size;
    private final long flags;
    private final Map<String, String> props;

    public VlmDfnPojo(
            final UUID uuid,
            final Integer volumeNr,
            final Integer minorNr,
            final long size,
            final long flags,
            Map<String, String> props)
    {
        this.uuid = uuid;
        this.volumeNr = volumeNr;
        this.minorNr = minorNr;
        this.size = size;
        this.flags = flags;
        this.props = props;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public Integer getVolumeNr() {
        return volumeNr;
    }

    @Override
    public Integer getMinorNr() {
        return minorNr;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public long getFlags() {
        return flags;
    }

    @Override
    public Map<String, String> getProps() {
        return props;
    }

}
