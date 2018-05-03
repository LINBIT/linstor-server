package com.linbit.linstor.event.generator.controller;

import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.generator.ResourceStateGenerator;
import com.linbit.linstor.event.generator.SatelliteStateHelper;
import com.linbit.linstor.satellitestate.SatelliteResourceState;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class CtrlResourceStateGenerator implements ResourceStateGenerator
{
    private final SatelliteStateHelper satelliteStateHelper;

    @Inject
    public CtrlResourceStateGenerator(
        SatelliteStateHelper satelliteStateHelperRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
    }

    @Override
    public Boolean generate(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        return satelliteStateHelper.withSatelliteState(
            objectIdentifier.getNodeName(),
            satelliteState -> satelliteState.getFromResource(
                objectIdentifier.getResourceName(),
                SatelliteResourceState::getReady
            ),
            null
        );
    }
}
