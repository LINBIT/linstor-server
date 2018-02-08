package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.api.pojo.VolumeState;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.RscStateOuterClass;
import com.linbit.linstor.proto.VlmStateOuterClass;
import com.linbit.linstor.security.AccessContext;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall
public class UpdateStates extends BaseProtoApiCall
{
    public UpdateStates(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_UPDATE_STATES;
    }

    @Override
    public String getDescription()
    {
        return "Updates state of satellite resources and volumes";
    }

    private ResourceState fromProtoRscState(RscStateOuterClass.RscState protoRscState)
    {
        Map<VolumeNumber, VolumeState> volumes = new TreeMap<>();

        for (VlmStateOuterClass.VlmState protoVlmState : protoRscState.getVlmStatesList())
        {
            try
            {
                VolumeNumber volNr = new VolumeNumber(protoVlmState.getVlmNr());
                volumes.put(volNr, new VolumeState(
                    volNr,
                    new MinorNumber(protoVlmState.getVlmMinorNr()),
                    protoVlmState.getIsPresent(),
                    protoVlmState.getHasDisk(),
                    protoVlmState.getHasMetaData(),
                    protoVlmState.getCheckMetaData(),
                    protoVlmState.getDiskFailed(),
                    protoVlmState.getNetSize(),
                    protoVlmState.getGrossSize()
                    )
                );
            }
            catch (ValueOutOfRangeException exc)
            {
                // ignore because this is sent from the satellite and creation of illegal
                // volume/minor numbers should never be possible
            }
        }

        ResourceState resourceState = new ResourceState(
            protoRscState.getRscName(),
            protoRscState.getNodeName(),
            protoRscState.getIsPresent(),
            protoRscState.getRequiresAdjust(),
            protoRscState.getIsPrimary(),
            volumes
        );

        return resourceState;
    }

    @Override
    public void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {

        Map<ResourceName, ResourceState> resourceStateMap = new HashMap<>();
        RscStateOuterClass.RscState protoRscState;
        while ((protoRscState = RscStateOuterClass.RscState.parseDelimitedFrom(msgDataIn)) != null)
        {
            ResourceState rscState = fromProtoRscState(protoRscState);
            try
            {
                resourceStateMap.put(new ResourceName(rscState.getRscName()), rscState);
            }
            catch (InvalidNameException exc)
            {
                // ignored should not happen
            }
        }
        client.setResourceStates(resourceStateMap);
    }
}
