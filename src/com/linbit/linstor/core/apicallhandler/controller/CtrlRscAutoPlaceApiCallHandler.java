package com.linbit.linstor.core.apicallhandler.controller;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import com.linbit.ImplementationError;
import com.linbit.linstor.Node;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.apicallhandler.AbsApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.Candidate;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.NotEnoughFreeNodesException;
import com.linbit.linstor.core.apicallhandler.controller.CtrlAutoStorPoolSelector.RuntimeAccessDeniedException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

public class CtrlRscAutoPlaceApiCallHandler extends AbsApiCallHandler
{
    private String currentRscName;

    private final ResourceDefinitionMap rscDfnMap;
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final CtrlRscApiCallHandler rscApiCallHandler;
    private final CtrlAutoStorPoolSelector autoStorPoolSelector;

    @Inject
    public CtrlRscAutoPlaceApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        @ApiContext AccessContext apiCtxRef,
        // @Named(ControllerSecurityModule.STOR_POOL_DFN_MAP_PROT) ObjectProtection storPoolDfnMapProtRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        CtrlObjectFactories objectFactories,
        CtrlRscApiCallHandler rscApiCallHandlerRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef,
        CtrlAutoStorPoolSelector autoStorPoolSelectorRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            LinStorObject.RESOURCE,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
        );
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
        rscApiCallHandler = rscApiCallHandlerRef;
        autoStorPoolSelector = autoStorPoolSelectorRef;
    }

    public ApiCallRc autoPlace(
        String rscNameStr,
        AutoSelectFilterApi selectFilter
    )
    {
        // TODO extract this method into an own interface implementation
        // that the controller can choose between different auto-place strategies
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.CREATE,
                apiCallRc,
                rscNameStr
            );
        )
        {
            // Using this try inside the other try is ugly.. hopefully this will
            // change wit the api-rework :)
            try
            {
                StorPoolName storPoolName = null;
                String storPoolNameStr = selectFilter.getStorPoolNameStr();
                if (storPoolNameStr != null)
                {
                    storPoolName = asStorPoolName(storPoolNameStr);
                }
                Candidate bestCandidate = autoStorPoolSelector.findBestCandidate(
                    calculateResourceDefinitionSize(rscNameStr),
                    selectFilter,
                    CtrlAutoStorPoolSelector::mostRemainingSpaceStrategy,
                    CtrlAutoStorPoolSelector::mostRemainingSpaceStrategy
                );

                Map<String, String> rscPropsMap = new TreeMap<>();
                rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, bestCandidate.storPoolName.displayValue);

                for (Node node : bestCandidate.nodes)
                {
                    rscApiCallHandler.createResource(
                        node.getName().displayValue,
                        rscNameStr,
                        Collections.emptyList(),
                        rscPropsMap,
                        Collections.emptyList(),
                        false, // createResource api should NOT autoClose the current transaction
                        // we will close it when we are finished with the autoPlace
                        apiCallRc
                    );
                }
                reportSuccess(
                    "Resource '" + rscNameStr + "' successfully autoplaced on " +
                        selectFilter.getPlaceCount() + " nodes",
                    "Used storage pool: '" + bestCandidate.storPoolName.displayValue + "'\n" +
                    "Used nodes: '" + bestCandidate.nodes.stream()
                        .map(node -> node.getName().displayValue)
                        .collect(Collectors.joining("', '")) + "'"
                );

            }
            catch (NotEnoughFreeNodesException nefnExc)
            {
                throw asExc(
                    nefnExc,
                    nefnExc.getMessage(),
                    nefnExc.getCauseText(),
                    nefnExc.getDetailsText(),
                    nefnExc.getCorrectionText(),
                    ApiConsts.FAIL_NOT_ENOUGH_NODES
                );
            }
            catch (InvalidKeyException exc)
            {
                throw asExc(
                    exc,
                    "The property key '" + exc.invalidKey + "' is invalid.",
                    ApiConsts.FAIL_INVLD_PROP
                );
            }
            catch (RuntimeAccessDeniedException runtimeAccdenidExc)
            {
                throw asAccDeniedExc(
                    runtimeAccdenidExc.getExc(),
                    runtimeAccdenidExc.getMsg(),
                    runtimeAccdenidExc.getRc()
                );
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.CREATE,
                getObjectDescriptionInline(rscNameStr),
                getObjRefs(rscNameStr),
                getVariables(rscNameStr),
                apiCallRc
            );
        }
        return apiCallRc;
    }

    private long calculateResourceDefinitionSize(String rscNameStr)
    {
        long size = 0;
        try
        {
            ResourceDefinitionData rscDfn = loadRscDfn(rscNameStr, true);
            Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn(peerAccCtx);
            while (vlmDfnIt.hasNext())
            {
                VolumeDefinition vlmDfn = vlmDfnIt.next();
                size += vlmDfn.getVolumeSize(peerAccCtx);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access " + CtrlRscDfnApiCallHandler.getObjectDescriptionInline(rscNameStr),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return size;
    }


    private AbsApiCallHandler setContext(
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        String rscNameStr
    )
    {
        super.setContext(
            type,
            apiCallRc,
            true,
            getObjRefs(rscNameStr),
            getVariables(rscNameStr)
        );
        currentRscName = rscNameStr;
        return this;
    }

    private Map<String, String> getObjRefs(String rscNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        return map;
    }

    private Map<String, String> getVariables(String rscNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_RSC_NAME, rscNameStr);
        return map;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Auto-placing resource: " + currentRscName;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentRscName);
    }

    private String getObjectDescriptionInline(String rscNameStr)
    {
        return "auto-placing resource: '" + rscNameStr + "'";
    }
}
