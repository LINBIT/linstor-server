package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.NodeInternalCallHandler;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntUpdateLocalNodeChangeOuterClass.MsgIntUpdateLocalNodeChange;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntUpdateLocalNodeChangeOuterClass.Props;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntUpdateLocalNodeChangeOuterClass.Set;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@ProtobufApiCall(
    name = InternalApiConsts.API_UPDATE_LOCAL_PROPS_FROM_STLT,
    description = "Called by the satellite to notify the controller that some node properties need change or deletion"
)
@Singleton
public class UpdateNodeChanges implements ApiCall
{
    private final NodeInternalCallHandler nodeInternalCallHandler;

    @Inject
    public UpdateNodeChanges(
        NodeInternalCallHandler nodeInternalCallHandlerRef
    )
    {
        nodeInternalCallHandler = nodeInternalCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntUpdateLocalNodeChange msg = MsgIntUpdateLocalNodeChange.parseDelimitedFrom(msgDataIn);
        Map<String, String> changedProps = msg.getChangedNodeProps().getPropsMap();
        List<String> deletedPropsList = msg.getDeletedNodeProps().getKeysList();

        Map<String, Map<String, String>> changedStorPoolProps = new HashMap<>();
        Map<String, List<String>> deletedStorPoolProps = new HashMap<>();
        for (Entry<String, Props> entry : msg.getChangedStorPoolPropsMap().entrySet())
        {
            changedStorPoolProps.put(entry.getKey(), entry.getValue().getPropsMap());
        }
        for (Entry<String, Set> entry : msg.getDeletedStorPoolPropsMap().entrySet())
        {
            deletedStorPoolProps.put(entry.getKey(), entry.getValue().getKeysList());
        }

        nodeInternalCallHandler.handleNodeUpdate(
            changedProps,
            deletedPropsList,
            changedStorPoolProps,
            deletedStorPoolProps
        );
    }
}
