package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.Property;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
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
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.locks.LockGuardFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashSet;
import java.util.List;

import org.slf4j.MDC;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlAuthResponseApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext sysCtx;

    private final CtrlFullSyncApiCallHandler ctrlFullSyncApiCallHandler;
    private final ReconnectorTask reconnectorTask;
    private final LockGuardFactory lockGuardFactory;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final NodeRepository nodeRepo;

    @Inject
    public CtrlAuthResponseApiCallHandler(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext sysCtxRef,
        CtrlFullSyncApiCallHandler ctrlFullSyncApiCallHandlerRef,
        ReconnectorTask reconnectorTaskRef,
        LockGuardFactory lockGuardFactoryRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        NodeRepository nodeRepositoryRef
    )
    {
        errorReporter = errorReporterRef;
        sysCtx = sysCtxRef;
        ctrlFullSyncApiCallHandler = ctrlFullSyncApiCallHandlerRef;
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
        Long expectedFullSyncId,
        String nodeUname,
        Integer linstorVersionMajor,
        Integer linstorVersionMinor,
        Integer linstorVersionPatch,
        List<ExtToolsInfo> externalToolsInfoList,
        StltConfig stltConfig,
        List<Property> dynamicPropListRef,
        boolean waitForFullSyncAnswerRef
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "authResponse",
            lockGuardFactory.buildDeferred(LockGuardFactory.LockType.WRITE, LockGuardFactory.LockObj.NODES_MAP),
            () -> authResponseInTransaction(
                peer,
                success,
                apiCallResponse,
                expectedFullSyncId,
                nodeUname,
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
        throws AccessDeniedException, InvalidValueException, DatabaseException
    {
        Node node = peer.getNode();
        Props nodeProps = node.getProps(sysCtx);
        @Nullable String oldUname = nodeProps.getProp(InternalApiConsts.NODE_UNAME);
        @Nullable NodeName curNodeName = nodeRepo.getUname(sysCtx, nodeUname);
        if (!nodeUname.equals(oldUname))
        {
            if (oldUname != null)
            {
                // uname change, cleanup old uname
                nodeRepo.removeUname(sysCtx, oldUname);
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
                nodeRepo.putUname(sysCtx, nodeUname, node.getName());
            }
        }
        else
        {
            if (curNodeName == null)
            {
                // reconnect node
                nodeRepo.putUname(sysCtx, nodeUname, node.getName());
            }
        }

    }

    private Flux<ApiCallRc> authResponseInTransaction(
        Peer peer,
        boolean success,
        ApiCallRcImpl apiCallResponse,
        Long expectedFullSyncId,
        String nodeUname,
        Integer linstorVersionMajor,
        Integer linstorVersionMinor,
        Integer linstorVersionPatch,
        List<ExtToolsInfo> externalToolsInfoList,
        StltConfig stltConfig,
        List<Property> dynamicPropListRef,
        boolean waitForFullSyncAnswerRef
    )
    {
        Flux<ApiCallRc> flux;

        if (success)
        {
            Node node = peer.getNode();
            if (LinStor.VERSION_INFO_PROVIDER.equalsVersion(
                    linstorVersionMajor,
                    linstorVersionMinor,
                    linstorVersionPatch
                )
            )
            {
                peer.setAuthenticated(true);
                peer.setConnectionStatus(ApiConsts.ConnectionStatus.CONNECTED);
                peer.getExtToolsManager().updateExternalToolsInfo(externalToolsInfoList);
                peer.setDynamicProperties(dynamicPropListRef);

                com.linbit.linstor.core.cfg.StltConfig stltCfg = new com.linbit.linstor.core.cfg.StltConfig();
                stltCfg.setConfigDir(stltConfig.getConfigDir());
                stltCfg.setDebugConsoleEnable(stltConfig.getDebugConsoleEnabled());
                stltCfg.setLogPrintStackTrace(stltConfig.getLogPrintStackTrace());
                stltCfg.setLogDirectory(stltConfig.getLogDirectory());
                stltCfg.setLogLevel(stltConfig.getLogLevel());
                stltCfg.setLogLevelLinstor(stltConfig.getLogLevelLinstor());
                stltCfg.setStltOverrideNodeName(stltConfig.getStltOverrideNodeName());
                stltCfg.setRemoteSpdk(stltConfig.getRemoteSpdk());
                stltCfg.setDrbdKeepResPattern(stltConfig.getDrbdKeepResPattern());
                stltCfg.setNetBindAddress(stltConfig.getNetBindAddress());
                stltCfg.setNetPort(stltConfig.getNetPort());
                stltCfg.setNetType(stltConfig.getNetType());
                stltCfg.setExternalFilesWhitelist(new HashSet<>(stltConfig.getWhitelistedExtFilePathsList()));
                peer.setStltConfig(stltCfg);

                logExternaltools(peer, nodeUname);

                // Set the satellite's access context
                // Certain APIs called by the satellite are executed with a privileged access context by the controller,
                // while the access context of the peer connection itself remains unprivileged
                AccessContext curCtx = peer.getAccessContext();
                AccessContext privCtx = sysCtx.clone();
                try
                {
                    privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                    // FIXME In the absence of any means of identification, assume the system identity for the peer.
                    // Set the SYSTEM identity on the Satellite's access context
                    AccessContext newCtx = privCtx.impersonate(
                        Identity.SYSTEM_ID, curCtx.subjectRole, curCtx.subjectDomain
                    );
                    // Disable all privileges on the Satellite's access context permanently
                    newCtx.getLimitPrivs().disablePrivileges(Privilege.PRIV_SYS_ALL);
                    peer.setAccessContext(privCtx, newCtx);

                    updateUnameMap(peer, nodeUname);

                    ctrlTransactionHelper.commit();
                }
                catch (AccessDeniedException accExc)
                {
                    errorReporter.reportError(
                        Level.ERROR,
                        new ImplementationError(
                            "Creation of an access context for a Satellite by the " +
                                privCtx.subjectRole.name.displayValue + " role failed",
                            accExc
                        )
                    );
                }
                catch (InvalidValueException | DatabaseException exc)
                {
                    errorReporter.reportError(exc);
                }
                errorReporter.logInfo("Satellite '" + node.getName() + "' authenticated");

                flux = ctrlFullSyncApiCallHandler.sendFullSync(
                    node,
                    expectedFullSyncId,
                    waitForFullSyncAnswerRef
                );

                try
                {
                    if (!node.getNodeType(sysCtx).isSpecial() &&
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
                catch (AccessDeniedException accExc)
                {
                    errorReporter.reportError(
                        Level.ERROR,
                        new ImplementationError(accExc)
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
