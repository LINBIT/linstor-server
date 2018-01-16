package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.ChildProcessTimeoutException;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntAuthOuterClass.MsgIntAuth;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class CtrlAuth extends BaseProtoApiCall
{
    private final Satellite satellite;

    public CtrlAuth(Satellite satellite)
    {
        super(satellite.getErrorReporter());
        this.satellite = satellite;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_AUTH;
    }

    @Override
    public String getDescription()
    {
        return "Called by the controller to authenticate the controller to the satellite";
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
        // TODO: implement authentication
        MsgIntAuth auth = MsgIntAuth.parseDelimitedFrom(msgDataIn);
        String nodeName = auth.getNodeName();
        UUID nodeUuid = UUID.fromString(auth.getNodeUuid());
        boolean authSuccess = true;
        ApiCallRcImpl apicallrc = new ApiCallRcImpl();

        // get satellites current hostname
        final ExtCmd extCommand = new ExtCmd(satellite.getTimer(), satellite.getErrorReporter());
        String hostname = "";
        try {
            final ExtCmd.OutputData output = extCommand.exec("uname", "-n");
            final String stdOut = new String(output.stdoutData);
            hostname = stdOut.trim();
        } catch (ChildProcessTimeoutException ex) {
            satellite.getErrorReporter().reportError(ex);
            authSuccess = false;
        }

        // Check if satellite hostname is equal to the given nodename
        if (!hostname.toLowerCase().equals(nodeName))
        {
            ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
            entry.setReturnCode(InternalApiConsts.API_AUTH_ERROR_HOST_MISMATCH);
            entry.setMessageFormat("Satellite node name doesn't match hostname.");
            String cause = String.format(
                    "Satellite node name '%s' doesn't match nodes hostname '%s'.",
                    nodeName,
                    hostname);
            entry.setCauseFormat(cause);
            apicallrc.addEntry(entry);

            satellite.getErrorReporter().logError(cause);
            authSuccess = false;
        }


        if (authSuccess)
        {
            // client auth was successful send API_AUTH_ACCEPT
            satellite.getErrorReporter().logInfo("Controller connected and authenticated");
            satellite.setControllerPeer(controllerPeer, nodeUuid, nodeName);

            byte[] msgAuthAccept = prepareMessage(
                accCtx,
                new byte[0],
                controllerPeer,
                msgId,
                InternalApiConsts.API_AUTH_ACCEPT
            );
            sendAnswer(controllerPeer, msgAuthAccept);
        }
        else
        {
            // some auth error happend, send error response
            byte[] msgData = prepareMessage(
                accCtx,
                createApiCallResponse(accCtx, apicallrc, controllerPeer),
                controllerPeer,
                msgId,
                InternalApiConsts.API_AUTH_ERROR
            );
            sendAnswer(controllerPeer, msgData);
        }
    }
}
