package com.linbit.linstor.event.generator.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.generator.ResourceDeploymentStateGenerator;
import com.linbit.linstor.event.generator.SatelliteStateHelper;
import com.linbit.linstor.satellitestate.SatelliteResourceState;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class CtrlResourceDeploymentStateGenerator implements ResourceDeploymentStateGenerator
{
    private final SatelliteStateHelper satelliteStateHelper;

    @Inject
    public CtrlResourceDeploymentStateGenerator(
        SatelliteStateHelper satelliteStateHelperRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
    }

    @Override
    public ApiCallRc generate(ObjectIdentifier objectIdentifier)
    {
        return satelliteStateHelper.withSatelliteState(
            objectIdentifier.getNodeName(),
            satelliteState -> satelliteState.getFromResource(
                objectIdentifier.getResourceName(),
                SatelliteResourceState::getDeploymentState
            ),
            null
        );
    }

    @Override
    public void clear(ObjectIdentifier objectIdentifier)
    {
        satelliteStateHelper.onSatelliteState(
            objectIdentifier.getNodeName(),
            satelliteState -> satelliteState.unsetOnResource(
                objectIdentifier.getResourceName(),
                SatelliteResourceState::setDeploymentState
            )
        );
    }
}
