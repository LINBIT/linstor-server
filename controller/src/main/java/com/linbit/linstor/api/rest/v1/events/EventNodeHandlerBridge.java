package com.linbit.linstor.api.rest.v1.events;

import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class EventNodeHandlerBridge extends EventHandlerBridge
{
    @Inject
    public EventNodeHandlerBridge(
        ErrorReporter errorReporterRef
    )
    {
        super(errorReporterRef);
    }

    private void sendNodeEvent(String eventName, NodeApi nodeApi)
    {
        JsonGenTypes.EventNode eventNode = new JsonGenTypes.EventNode();
        eventNode.node = Json.apiToNode(nodeApi);
        sendEvent(eventName, eventNode);
    }

    public void triggerNodeCreate(NodeApi nodeApi)
    {
        sendNodeEvent("node-create", nodeApi);
    }

    public void triggerNodeDelete(NodeApi nodeApi)
    {
        sendNodeEvent("node-delete", nodeApi);
    }

    public void triggerNodeEvacuate(NodeApi nodeApi)
    {
        sendNodeEvent("node-evacuate", nodeApi);
    }

    public void triggerNodeEvicted(NodeApi nodeApi)
    {
        sendNodeEvent("node-evicted", nodeApi);
    }

    public void triggerNodeRestored(NodeApi nodeApi)
    {
        sendNodeEvent("node-restored", nodeApi);
    }

    public void triggerNodeModified(NodeApi oldNode, NodeApi newNode)
    {
        JsonGenTypes.EventNodeModified nodeModified = new JsonGenTypes.EventNodeModified();
        nodeModified.old_node = Json.apiToNode(oldNode);
        nodeModified.new_node = Json.apiToNode(newNode);
        sendEvent("node-modified", nodeModified);
    }
}
