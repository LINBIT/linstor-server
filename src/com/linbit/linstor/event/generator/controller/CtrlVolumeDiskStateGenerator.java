package com.linbit.linstor.event.generator.controller;

import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.generator.SatelliteStateHelper;
import com.linbit.linstor.event.generator.VolumeDiskStateGenerator;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class CtrlVolumeDiskStateGenerator implements VolumeDiskStateGenerator
{
    private final SatelliteStateHelper satelliteStateHelper;

    @Inject
    public CtrlVolumeDiskStateGenerator(
        SatelliteStateHelper satelliteStateHelperRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
    }

    @Override
    public String generate(ObjectIdentifier objectIdentifier)
    {
        return satelliteStateHelper.withSatelliteState(
            objectIdentifier.getNodeName(),
            satelliteState -> satelliteState.getFromVolume(
                objectIdentifier.getResourceName(),
                objectIdentifier.getVolumeNumber(),
                SatelliteVolumeState::getDiskState
            ),
            "NoSatelliteState"
        );
    }
}
