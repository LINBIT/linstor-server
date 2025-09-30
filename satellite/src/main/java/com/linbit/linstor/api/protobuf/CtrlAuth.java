package com.linbit.linstor.api.protobuf;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.UpdateMonitor;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.core.apicallhandler.satellite.authentication.AuthenticationResult;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntAuthOuterClass.MsgIntAuth;
import com.linbit.linstor.utils.SetUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_AUTH,
    description = "Called by the controller to authenticate the controller to the satellite",
    requiresAuth = false
)
@Singleton
public class CtrlAuth implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final StltApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;
    private final CommonSerializer commonSerializer;
    private final UpdateMonitor updateMonitor;
    private final Provider<Peer> controllerPeerProvider;
    private final ExtCmdFactory extCmdFactory;
    private final StltConfig stltConfig;
    private final WhitelistProps whitelistProps;

    @Inject
    public CtrlAuth(
        ErrorReporter errorReporterRef,
        StltApiCallHandler apiCallHandlerRef,
        ApiCallAnswerer apiCallAnswererRef,
        CommonSerializer commonSerializerRef,
        UpdateMonitor updateMonitorRef,
        Provider<Peer> controllerPeerProviderRef,
        ExtCmdFactory extCmdFactoryRef,
        StltConfig stltConfigRef,
        WhitelistProps whitelistPropsRef
    )
    {
        errorReporter = errorReporterRef;
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
        commonSerializer = commonSerializerRef;
        updateMonitor = updateMonitorRef;
        controllerPeerProvider = controllerPeerProviderRef;
        extCmdFactory = extCmdFactoryRef;
        stltConfig = stltConfigRef;
        whitelistProps = whitelistPropsRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        // get the host uname for the drbd config
        String nodeUname = LinStor.getHostName();

        // TODO: implement authentication
        MsgIntAuth auth = MsgIntAuth.parseDelimitedFrom(msgDataIn);
        String nodeName = auth.getNodeName();
        UUID nodeUuid = ProtoUuidUtils.deserialize(auth.getNodeUuid());

        Peer controllerPeer = controllerPeerProvider.get();
        UUID ctrlUuid = null;
        if (auth.getCtrlUuid() != null)
        {
            ctrlUuid = ProtoUuidUtils.deserialize(auth.getCtrlUuid());
        }
        if (ctrlUuid == null)
        {
            errorReporter.logWarning("Controller sent invalid ctrl-UUID: '%s'", auth.getCtrlUuid());
        }

        AuthenticationResult authResult = apiCallHandler.authenticate(nodeUuid, nodeName, controllerPeer, ctrlUuid);

        byte[] replyBytes;
        if (authResult.isAuthenticated())
        {
            // all ok, send the new fullSyncId with the AUTH_ACCEPT msg
            // additionally we also send information which layers are supported by the current satellite

            replyBytes = commonSerializer.headerlessBuilder()
                .authSuccess(
                    updateMonitor.getNextFullSyncId(),
                    LinStor.VERSION_INFO_PROVIDER.getSemanticVersion(),
                    nodeUname,
                    authResult.getExternalToolsInfoList(),
                    authResult.getApiCallRc(),
                    stltConfig.getConfigDir(),
                    stltConfig.isDebugConsoleEnabled(),
                    stltConfig.isLogPrintStackTrace(),
                    stltConfig.getLogDirectory(),
                    stltConfig.getLogLevel(),
                    stltConfig.getLogLevelLinstor(),
                    stltConfig.getStltOverrideNodeName(),
                    stltConfig.isRemoteSpdk(),
                    stltConfig.isEbs(),
                    stltConfig.getNetBindAddress(),
                    stltConfig.getNetPort(),
                    stltConfig.getNetType(),
                    SetUtils.convertPathsToStrings(stltConfig.getWhitelistedExternalFilePaths()),
                    whitelistProps
                )
                .build();
        }
        else
        {
            // whatever happened should be in the apiCallRc
            replyBytes = commonSerializer.headerlessBuilder()
                .authError(authResult.getApiCallRc())
                .build();
        }
        controllerPeerProvider.get().sendMessage(
            apiCallAnswerer.answerBytes(
                replyBytes,
                InternalApiConsts.API_AUTH_RESPONSE
            ),
            InternalApiConsts.API_AUTH_RESPONSE
        );
    }
}
