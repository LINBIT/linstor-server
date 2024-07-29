package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
class CtrlRscConnectionApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlRscConnectionHelper ctrlRscConnectionHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;

    @Inject
    CtrlRscConnectionApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlRscConnectionHelper ctrlRscConnectionHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        ErrorReporter errorReporterRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlRscConnectionHelper = ctrlRscConnectionHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        errorReporter = errorReporterRef;
    }

    public Flux<ApiCallRc> createResourceConnection(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        Map<String, String> rscConnPropsMap
    )
    {
        List<Flux<Flux<ApiCallRc>>> fluxes = new ArrayList<>();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        ResponseContext context = makeResourceConnectionContext(
            ApiOperation.makeCreateOperation(),
            nodeName1Str,
            nodeName2Str,
            rscNameStr
        );

        try
        {
            ResourceConnection rscConn =
                ctrlRscConnectionHelper.createRscConn(nodeName1Str, nodeName2Str, rscNameStr, null);

            ctrlPropsHelper.fillProperties(
                apiCallRcs,
                LinStorObject.RSC_CONN,
                rscConnPropsMap,
                getProps(rscConn),
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN);

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(apiCallRcs, context, ApiSuccessUtils.defaultCreatedEntry(
                rscConn.getUuid(), getResourceConnectionDescriptionInline(apiCtx, rscConn)));

            fluxes = updateSatellites(rscConn);
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.just((ApiCallRc) apiCallRcs)
            .concatWith(CtrlResponseUtils.mergeExtractingApiRcExceptions(errorReporter, Flux.merge(fluxes)));
    }

    public Flux<ApiCallRc> modify(
        @Nullable UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces
    )
    {
        ResponseContext context = makeResourceConnectionContext(
            ApiOperation.makeModifyOperation(),
            nodeName1,
            nodeName2,
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Modify resource-connection",
                lockGuardFactory.buildDeferred(WRITE, NODES_MAP, RSC_DFN_MAP),
                () -> modifyInTransaction(
                    rscConnUuid,
                    nodeName1,
                    nodeName2,
                    rscNameStr,
                    overrideProps,
                    deletePropKeys,
                    deletePropNamespaces,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> modifyInTransaction(
        @Nullable UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces,
        ResponseContext context
    )
    {
        List<Flux<Flux<ApiCallRc>>> fluxes = new ArrayList<>();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        boolean notifyStlts;

        try
        {
            ResourceConnection rscConn =
                ctrlRscConnectionHelper.loadOrCreateRscConn(rscConnUuid, nodeName1, nodeName2, rscNameStr);

            for (String key : overrideProps.keySet())
            {
                if (key.startsWith(ApiConsts.NAMESPC_CONNECTION_PATHS + "/"))
                {
                    if (key.matches(ApiConsts.NAMESPC_CONNECTION_PATHS + "/.*/.*"))
                    {
                        // check node name is correct
                        int lastSlash = key.lastIndexOf('/');
                        String nodeName = key.substring(lastSlash + 1);
                        if (!nodeName.equals(nodeName1) && !nodeName.equals(nodeName2))
                        {
                            throw new ApiRcException(ApiCallRcImpl
                                .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Connection path node unknown.")
                                .setCause("The node name '" + nodeName + "' is unknown.")
                                .build()
                            );
                        }

                        // now check that the interface name are correct/existing
                        String netIfName = overrideProps.get(key);
                        Node node = rscConn.getNode(new NodeName(nodeName));
                        NetInterface netInterface = node.getNetInterface(
                            peerAccCtx.get(),
                            new NetInterfaceName(netIfName)
                        );
                        if (netInterface == null)
                        {
                            throw new ApiRcException(ApiCallRcImpl
                                .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "NetInterface for node unknown.")
                                .setCause(
                                    String.format(
                                        "The Netinterface '%s' is not known for node '%s'",
                                        netIfName, nodeName
                                    )
                                )
                                .build()
                            );
                        }
                    }
                    else
                    {
                        throw new ApiRcException(ApiCallRcImpl
                            .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Connection path property invalid.")
                            .setCause("The key '" + key + "' is invalid.")
                            .build()
                        );
                    }
                }
            }

            Props props = getProps(rscConn);
            List<String> keysIgnored = new ArrayList<>();
            keysIgnored.add(ApiConsts.NAMESPC_CONNECTION_PATHS + "/");

            notifyStlts = ctrlPropsHelper.fillProperties(
                apiCallRcs,
                LinStorObject.RSC_CONN,
                overrideProps,
                getProps(rscConn),
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN,
                keysIgnored
            );
            notifyStlts = ctrlPropsHelper.remove(
                apiCallRcs,
                LinStorObject.RSC_CONN,
                props,
                deletePropKeys,
                deletePropNamespaces,
                keysIgnored
            ) || notifyStlts;

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(apiCallRcs, context, ApiSuccessUtils.defaultModifiedEntry(
                rscConn.getUuid(), getResourceConnectionDescriptionInline(apiCtx, rscConn)));

            if (notifyStlts)
            {
                fluxes = updateSatellites(rscConn);
            }
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.just((ApiCallRc) apiCallRcs)
            .concatWith(CtrlResponseUtils.mergeExtractingApiRcExceptions(errorReporter, Flux.merge(fluxes)));
    }

    public Flux<ApiCallRc> deleteResourceConnection(
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr
    )
    {
        List<Flux<Flux<ApiCallRc>>> fluxes = new ArrayList<>();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        ResponseContext context = makeResourceConnectionContext(
            ApiOperation.makeDeleteOperation(),
            nodeName1Str,
            nodeName2Str,
            rscNameStr
        );

        try
        {
            ResourceConnection rscConn =
                ctrlRscConnectionHelper.loadRscConn(nodeName1Str, nodeName2Str, rscNameStr, true);
            UUID rscConnUuid = rscConn.getUuid();
            delete(rscConn);

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(apiCallRcs, context, ApiSuccessUtils.defaultDeletedEntry(
                rscConnUuid, getResourceConnectionDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr)));

            fluxes = updateSatellites(rscConn);
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.just((ApiCallRc) apiCallRcs)
            .concatWith(CtrlResponseUtils.mergeExtractingApiRcExceptions(errorReporter, Flux.merge(fluxes)));
    }

    private Props getProps(ResourceConnection rscConn)
    {
        Props props;
        try
        {
            props = rscConn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties of " + getResourceConnectionDescriptionInline(apiCtx, rscConn),
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
        }
        return props;
    }

    private List<Flux<Flux<ApiCallRc>>> updateSatellites(ResourceConnection rscConn)
    {
        List<Flux<Flux<ApiCallRc>>> fluxes = new ArrayList<>();

        try
        {
            fluxes.add(Flux.just(ctrlSatelliteUpdateCaller
                .updateSatellites(rscConn.getSourceResource(apiCtx), Flux.empty())
                .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2())
            ));

            fluxes.add(Flux.just(ctrlSatelliteUpdateCaller
                .updateSatellites(rscConn.getTargetResource(apiCtx), Flux.empty())
                .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2())
            ));
        }
        catch (AccessDeniedException implErr)
        {
            throw new ImplementationError(implErr);
        }

        return fluxes;
    }

    private void delete(ResourceConnection rscConn)
    {
        try
        {
            rscConn.delete(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete " + getResourceConnectionDescriptionInline(apiCtx, rscConn),
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    public static String getResourceConnectionDescription(String nodeName1, String nodeName2, String rscName)
    {
        return "Resource connection between nodes " + nodeName1 + " and " +
            nodeName2 + " for resource " + rscName;
    }

    public static String getResourceConnectionDescriptionInline(AccessContext accCtx, ResourceConnection rscConn)
    {
        String descriptionInline;
        try
        {
            descriptionInline = getResourceConnectionDescriptionInline(
                rscConn.getSourceResource(accCtx).getNode().getName().displayValue,
                rscConn.getTargetResource(accCtx).getNode().getName().displayValue,
                rscConn.getSourceResource(accCtx).getResourceDefinition().getName().displayValue
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return descriptionInline;
    }

    public static String getResourceConnectionDescriptionInline(String nodeName1, String nodeName2, String rscName)
    {
        return "resource connection between nodes '" + nodeName1 + "' and '" +
            nodeName2 + "' for resource '" + rscName + "'";
    }

    static ResponseContext makeResourceConnectionContext(
        ApiOperation operation,
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_1ST_NODE, nodeName1Str);
        objRefs.put(ApiConsts.KEY_2ND_NODE, nodeName2Str);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        return new ResponseContext(
            operation,
            getResourceConnectionDescription(nodeName1Str, nodeName2Str, rscNameStr),
            getResourceConnectionDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr),
            ApiConsts.MASK_RSC_CONN,
            objRefs
        );
    }
}
