package com.linbit.linstor.api.rest.v1.events;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collections;

import org.glassfish.jersey.media.sse.EventOutput;

@Singleton
public class EventDrbdHandlerBridge extends EventHandlerBridge
{
    private final CtrlApiCallHandler ctrlApiCallHandler;

    @Inject
    public EventDrbdHandlerBridge(
        ErrorReporter errorReporterRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef
    )
    {
        super(errorReporterRef);
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
    }

    public void registerResourceClient(EventOutput eventOut, boolean sendInitialState)
    {
        registerResourceClient(eventOut);

        if (sendInitialState)
        {
            ResourceList resourceList = ctrlApiCallHandler.listResource(
                Collections.emptyList(), Collections.emptyList());

            resourceList.getResources().forEach(
                rsc ->
                {
                    if (rsc.getLayerData().getLayerKind() == DeviceLayerKind.DRBD)
                    {
                        DrbdRscPojo drbdRscPojo = (DrbdRscPojo) rsc.getLayerData();
                        triggerMayPromote(rsc, drbdRscPojo.mayPromote());
                    }
                }
            );
        }
    }

    public void triggerMayPromote(final ResourceApi rsc, final @Nullable Boolean mayPromote)
    {
        JsonGenTypes.EventMayPromoteChange eventMayPromoteChange = new JsonGenTypes.EventMayPromoteChange();
        eventMayPromoteChange.node_name = rsc.getNodeName();
        eventMayPromoteChange.resource_name = rsc.getName();
        eventMayPromoteChange.may_promote = mayPromote;
        sendEvent("may-promote-change", eventMayPromoteChange);
    }
}
