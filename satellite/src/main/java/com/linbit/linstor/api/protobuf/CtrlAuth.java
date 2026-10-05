package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.core.apicallhandler.satellite.authentication.AuthenticationResult;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntAuthOuterClass.MsgIntAuth;
import com.linbit.linstor.utils.SetUtils;
import com.linbit.Platform;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

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
    private final Provider<Peer> controllerPeerProvider;
    private final StltConfig stltConfig;
    private final WhitelistProps whitelistProps;

    @Inject
    public CtrlAuth(
        ErrorReporter errorReporterRef,
        StltApiCallHandler apiCallHandlerRef,
        ApiCallAnswerer apiCallAnswererRef,
        CommonSerializer commonSerializerRef,
        Provider<Peer> controllerPeerProviderRef,
        StltConfig stltConfigRef,
        WhitelistProps whitelistPropsRef
    )
    {
        errorReporter = errorReporterRef;
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
        commonSerializer = commonSerializerRef;
        controllerPeerProvider = controllerPeerProviderRef;
        stltConfig = stltConfigRef;
        whitelistProps = whitelistPropsRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        // get the host uname for the drbd config
        String nodeUname = LinStor.getHostName();

        AuthenticationResult authResult;
        @Nullable ApiConsts.Platform platform = Platform.apiPlatform();
        @Nullable String osVariant = Platform.osVariant();
        try
        {
            // TODO: implement authentication
            MsgIntAuth auth = MsgIntAuth.parseDelimitedFrom(msgDataIn);
            String nodeName = auth.getNodeName();
            UUID nodeUuid = ProtoUuidUtils.deserialize(auth.getNodeUuid());

            Peer controllerPeer = controllerPeerProvider.get();
            UUID ctrlUuid = ProtoUuidUtils.deserialize(auth.getCtrlUuid());

            authResult = apiCallHandler.authenticate(nodeUuid, nodeName, controllerPeer, ctrlUuid);
        }
        catch (Exception exc)
        {
            // includes parsing exception from very different controller
            String details = "Failed to parse message. If this was an attempt from a LINSTOR controller " +
                "please check if the controller has the same version as the satellite. Satellite version: " +
                LinStor.VERSION_INFO_PROVIDER.getVersion();
            @Nullable String reportErrorId = errorReporter.reportError(exc, null, details);
            ApiCallRcImpl apiCallRcImpl = new ApiCallRcImpl();
            apiCallRcImpl.add(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.UNKNOWN_API_CALL,
                    "Failed to authenticate. Error ID: " + reportErrorId)
                    .setDetails(details)
                    .build()
            );
            authResult = new AuthenticationResult(apiCallRcImpl);
        }

        Peer controllerPeer = controllerPeerProvider.get();
        @Nullable byte[] replyBytes = null;
        if (authResult.isAuthenticated())
        {
            /*
             * Only draw the next fullSyncId if we are still the active controller connection. If another
             * connection authenticated in the meantime (concurrent Auths during a "double reconnect"), our
             * connection has already been closed: drawing a fullSyncId now would invalidate the id that was
             * (or will be) sent over the other, still living connection, while our AUTH_ACCEPT would be sent
             * into the closed connection and never reach the controller. The satellite would then wait forever
             * for a FullSync with an id the controller never received.
             */
            @Nullable Long nextFullSyncId = apiCallHandler.getNextFullSyncId(controllerPeer);
            if (nextFullSyncId == null)
            {
                errorReporter.logWarning(
                    "Skipping AUTH_ACCEPT for connection %s since a different controller connection " +
                        "authenticated in the meantime",
                    controllerPeer.getId()
                );
            }
            else
            {
                // all ok, send the new fullSyncId with the AUTH_ACCEPT msg
                // additionally we also send information which layers are supported by the current satellite

                replyBytes = commonSerializer.headerlessBuilder()
                    .authSuccess(
                        nextFullSyncId,
                        LinStor.VERSION_INFO_PROVIDER.getSemanticVersion(),
                        nodeUname,
                        platform,
                        osVariant,
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
        }
        else
        {
            // whatever happened should be in the apiCallRc
            replyBytes = commonSerializer.headerlessBuilder()
                .authError(authResult.getApiCallRc())
                .build();
        }
        if (replyBytes != null)
        {
            controllerPeer.sendMessage(
                apiCallAnswerer.answerBytes(
                    replyBytes,
                    InternalApiConsts.API_AUTH_RESPONSE
                ),
                InternalApiConsts.API_AUTH_RESPONSE
            );
        }
    }
}
