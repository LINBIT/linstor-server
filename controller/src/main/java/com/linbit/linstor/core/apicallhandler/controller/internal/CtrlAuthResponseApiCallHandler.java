package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.Property;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAuthHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.common.StltConfigOuterClass.StltConfig;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.locks.LockGuardFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.HashSet;
import java.util.List;

import org.slf4j.MDC;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlAuthResponseApiCallHandler
{
    private final ErrorReporter errorReporter;

    private final CtrlFullSyncApiCallHandler ctrlFullSyncApiCallHandler;
    private final CtrlAuthHandler ctrlAuthHandler;
    private final ReconnectorTask reconnectorTask;
    private final LockGuardFactory lockGuardFactory;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final NodeRepository nodeRepo;

    @Inject
    public CtrlAuthResponseApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlFullSyncApiCallHandler ctrlFullSyncApiCallHandlerRef,
        CtrlAuthHandler ctrlAuthHandlerRef,
        ReconnectorTask reconnectorTaskRef,
        LockGuardFactory lockGuardFactoryRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        NodeRepository nodeRepositoryRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlFullSyncApiCallHandler = ctrlFullSyncApiCallHandlerRef;
        ctrlAuthHandler = ctrlAuthHandlerRef;
        reconnectorTask = reconnectorTaskRef;
        lockGuardFactory = lockGuardFactoryRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        nodeRepo = nodeRepositoryRef;
    }

    public Flux<ApiCallRc> authResponse(
        Peer peer,
        boolean success,
        ApiCallRcImpl apiCallResponse,
        @Nullable Long expectedFullSyncId,
        @Nullable String nodeUname,
        @Nullable ApiConsts.Platform platform,
        @Nullable String osVariant,
        @Nullable Integer linstorVersionMajor,
        @Nullable Integer linstorVersionMinor,
        @Nullable Integer linstorVersionPatch,
        @Nullable List<ExtToolsInfo> externalToolsInfoList,
        @Nullable StltConfig stltConfig,
        List<Property> dynamicPropListRef,
        boolean waitForFullSyncAnswerRef
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "authResponse",
            lockGuardFactory.buildDeferred(
                LockGuardFactory.LockType.WRITE,
                LockGuardFactory.LockObj.NODES_MAP,
                LockGuardFactory.LockObj.AUTH_TOKEN_MAP
            ),
            () -> authResponseInTransaction(
                peer,
                success,
                apiCallResponse,
                expectedFullSyncId,
                nodeUname,
                platform,
                osVariant,
                linstorVersionMajor,
                linstorVersionMinor,
                linstorVersionPatch,
                externalToolsInfoList,
                stltConfig,
                dynamicPropListRef,
                waitForFullSyncAnswerRef
            ),
            MDC.getCopyOfContextMap()
        );
    }

    private void updateUnameMap(Peer peer, String nodeUname)
        throws InvalidValueException, DatabaseException
    {
        Node node = peer.getNode();
        Props nodeProps = node.getProps();
        @Nullable String oldUname = nodeProps.getProp(InternalApiConsts.NODE_UNAME);
        @Nullable NodeName curNodeName = nodeRepo.getUname(nodeUname);
        if (!nodeUname.equals(oldUname))
        {
            if (oldUname != null)
            {
                // uname change, cleanup old uname
                nodeRepo.removeUname(oldUname);
            }
            if (curNodeName != null)
            {
                peer.setAuthenticated(false);
                peer.setConnectionStatus(ApiConsts.ConnectionStatus.DUPLICATE_UNAME);
                errorReporter.reportError(
                    Level.ERROR,
                    new InvalidNameException(
                        String.format(
                            "Satellite has an uname '%s' that is already used by a different satellite '%s'",
                            nodeUname,
                            curNodeName),
                        nodeUname
                    )
                );
            }
            else
            {
                // new node added
                nodeProps.setProp(InternalApiConsts.NODE_UNAME, nodeUname);
                nodeRepo.putUname(nodeUname, node.getName());
            }
        }
        else
        {
            if (curNodeName == null)
            {
                // reconnect node
                nodeRepo.putUname(nodeUname, node.getName());
            }
        }

    }

    private Flux<ApiCallRc> authResponseInTransaction(
        Peer peer,
        boolean successRef,
        ApiCallRcImpl apiCallResponse,
        @Nullable Long expectedFullSyncId,
        @Nullable String nodeUname,
        @Nullable ApiConsts.Platform platform,
        @Nullable String osVariant,
        @Nullable Integer linstorVersionMajor,
        @Nullable Integer linstorVersionMinor,
        @Nullable Integer linstorVersionPatch,
        @Nullable List<ExtToolsInfo> externalToolsInfoList,
        @Nullable StltConfig stltConfig,
        List<Property> dynamicPropListRef,
        boolean waitForFullSyncAnswerRef
    )
    {
        @Nullable Node peersNode = peer.getNode();
        if (peersNode == null || peersNode.isDeleted() || !peersNode.getPeer().equals(peer))
        {
            /*
             * The connection this auth response arrived on (or was sent for) has already been replaced by a
             * newer connection ("double reconnect"). The response's expectedFullSyncId was (or will be)
             * invalidated by the newer connection's authentication - authenticating this peer or sending a
             * FullSync based on this response would be wrong. The newer connection runs its own handshake.
             */
            errorReporter.logWarning(
                "Ignoring authentication response of an already replaced connection (%s)",
                peer
            );
            return Flux.empty();
        }

        Flux<ApiCallRc> flux;
        boolean success = successRef;
        boolean matchVersion;
        if (linstorVersionMajor == null || linstorVersionMinor == null || linstorVersionPatch == null)
        {
            success = false;
            matchVersion = false;
            errorReporter.logError(
                "Peer %s responded with a broken version number: %s.%s.%s",
                peer.getNode(),
                linstorVersionMajor,
                linstorVersionMinor,
                linstorVersionPatch
            );
        }
        else
        {
            matchVersion = LinStor.VERSION_INFO_PROVIDER.equalsVersion(
                linstorVersionMajor,
                linstorVersionMinor,
                linstorVersionPatch
            );
        }

        if (success)
        {
            Node node = peer.getNode();
            if (matchVersion)
            {
                peer.setAuthenticated(true);
                peer.setConnectionStatus(ApiConsts.ConnectionStatus.CONNECTED);
                peer.getExtToolsManager().updateExternalToolsInfo(externalToolsInfoList);
                peer.setDynamicProperties(dynamicPropListRef);
                peer.setPlatform(platform);
                peer.setOsVariant(osVariant);

                com.linbit.linstor.core.cfg.StltConfig stltCfg = new com.linbit.linstor.core.cfg.StltConfig();
                stltCfg.setConfigDir(stltConfig.getConfigDir());
                stltCfg.setDebugConsoleEnable(stltConfig.getDebugConsoleEnabled());
                stltCfg.setLogPrintStackTrace(stltConfig.getLogPrintStackTrace());
                stltCfg.setLogDirectory(stltConfig.getLogDirectory());
                stltCfg.setLogLevel(stltConfig.getLogLevel());
                stltCfg.setLogLevelLinstor(stltConfig.getLogLevelLinstor());
                stltCfg.setStltOverrideNodeName(stltConfig.getStltOverrideNodeName());
                stltCfg.setRemoteSpdk(stltConfig.getRemoteSpdk());
                stltCfg.setNetBindAddress(stltConfig.getNetBindAddress());
                stltCfg.setNetPort(stltConfig.getNetPort());
                stltCfg.setNetType(stltConfig.getNetType());
                stltCfg.addToExternalFilesWhitelist(new HashSet<>(stltConfig.getWhitelistedExtFilePathsList()));
                peer.setStltConfig(stltCfg);

                logExternaltools(peer, nodeUname);

                try
                {
                    updateUnameMap(peer, nodeUname);

                    ctrlTransactionHelper.commit();
                }
                catch (InvalidValueException | DatabaseException exc)
                {
                    errorReporter.reportError(exc);
                }
                errorReporter.logInfo("Satellite '" + node.getName() + "' authenticated");

                // Send auth token to satellite if token auth is enabled
                if (ctrlAuthHandler.isTokenAuthEnabled())
                {
                    ctrlAuthHandler.ensureAndSendSatelliteToken(node);
                }

                flux = ctrlFullSyncApiCallHandler.sendFullSync(
                    node,
                    expectedFullSyncId,
                    waitForFullSyncAnswerRef
                );

                if (!node.getNodeType().isSpecial() &&
                    !nodeUname.equalsIgnoreCase(node.getName().displayValue))
                {
                    flux = flux.concatWith(
                        Flux.just(
                            ApiCallRcImpl.singleApiCallRc(
                                ApiConsts.INFO_NODE_NAME_MISMATCH,
                                String.format(
                                    "Linstor node name '%s' and hostname '%s' doesn't match.",
                                    node.getName().displayValue,
                                    nodeUname
                                )
                            )
                        )
                    );
                }
            }
            else
            {
                peer.setConnectionStatus(ApiConsts.ConnectionStatus.VERSION_MISMATCH);
                errorReporter.logError(
                    String.format(
                        "Satellite '%s' version mismatch(v%d.%d.%d).",
                        node.getName(),
                        linstorVersionMajor,
                        linstorVersionMinor,
                        linstorVersionPatch
                    )
                );
                peer.closeConnection();

                reconnectorTask.add(peer, false);

                flux = Flux.empty();
            }
        }
        else
        {
            peer.setAuthenticated(false);

            peer.setConnectionStatus(ApiConsts.ConnectionStatus.AUTHENTICATION_ERROR);

            for (RcEntry entry : apiCallResponse)
            {
                errorReporter.logError(" * " + entry.getCause());
            }

            flux = Flux.empty();
        }
        return flux;
    }

    private void logExternaltools(Peer peerRef, String nodeUnameRef)
    {
        String nodeName = peerRef.getNode().getName().displayValue;
        errorReporter.logDebug("%s, uname: %s", peerRef.toString(), nodeUnameRef);
        ExtToolsManager extToolsManager = peerRef.getExtToolsManager();
        for (ExtTools extTool : ExtTools.values())
        {
            ExtToolsInfo extToolInfo = extToolsManager.getExtToolInfo(extTool);
            if (extToolInfo == null)
            {
                errorReporter.logDebug("%s, %s: not available", nodeName, extTool.name());
            }
            else
            {
                if (!extToolInfo.isSupported())
                {
                    errorReporter.logDebug("%s, %s: not supported", nodeName, extTool.name());
                    for (String reason : extToolInfo.getNotSupportedReasons())
                    {
                        errorReporter.logDebug("%s,  %s: %s", nodeName, extTool.name(), reason);
                    }
                }
                else
                {
                    errorReporter.logDebug(
                        "%s, %s: %d.%d.%d",
                        nodeName,
                        extTool.name(),
                        extToolInfo.getVersionMajor(),
                        extToolInfo.getVersionMinor(),
                        extToolInfo.getVersionPatch()
                    );
                }
            }
        }
    }
}
