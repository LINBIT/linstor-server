package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ChangedStorPool extends BaseProtoApiCall
{
    private Satellite satellite;

    public ChangedStorPool(Satellite satellite)
    {
        super(satellite.getErrorReporter());
        this.satellite = satellite;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_CHANGED_STOR_POOL;
    }

    @Override
    public String getDescription()
    {
        return "Controller calls this API when a resource has changed and this satellite should " +
            "ask for the change";
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer controllerPeer
    )
        throws IOException
    {
        try
        {
            MsgIntObjectId storPoolId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
            String storPoolName = storPoolId.getName();
            UUID storPoolUuid = BaseProtoApiCall.asUuid(storPoolId.getUuid());

            Map<NodeName, UUID> nodesUpd = new TreeMap<>();
            nodesUpd.put(satellite.getLocalNode().getName(), storPoolUuid);
            satellite.getDeviceManager().getUpdateTracker().updateStorPool(
                storPoolUuid,
                new StorPoolName(storPoolName)
            );
        }
        catch (InvalidNameException invalidNameExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Controller sent an invalid stor pool name",
                    invalidNameExc
                )
            );
        }
    }

}
