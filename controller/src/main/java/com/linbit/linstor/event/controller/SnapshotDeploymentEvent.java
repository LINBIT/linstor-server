package com.linbit.linstor.event.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.event.GenericEvent;
import com.linbit.linstor.event.LinstorTriggerableEvent;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SnapshotDeploymentEvent
{
    private final LinstorTriggerableEvent<ApiCallRc> event;

    @Inject
    public SnapshotDeploymentEvent(GenericEvent<ApiCallRc> eventRef)
    {
        event = eventRef;
    }

    public LinstorTriggerableEvent<ApiCallRc> get()
    {
        return event;
    }
}
