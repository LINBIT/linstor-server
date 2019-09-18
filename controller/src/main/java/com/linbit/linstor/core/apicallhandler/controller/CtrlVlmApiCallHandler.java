package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class CtrlVlmApiCallHandler
{
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResponseConverter responseConverter;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final Provider<Peer> peer;

    @Inject
    public CtrlVlmApiCallHandler(
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef
    )
    {
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
    }

    public ApiCallRc modifyVolume(
        UUID vlmUuid,
        String nodeNameStr,
        String rscNameStr,
        Integer vlmNrInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespacesRef
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeVlmContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            rscNameStr,
            vlmNrInt
        );

        try
        {
            Volume vlm = ctrlApiDataLoader.loadVlm(nodeNameStr, rscNameStr, vlmNrInt, true);

            if (vlmUuid != null && !vlmUuid.equals(vlm.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_VLM,
                    "UUID-check failed"
                ));
            }

            Props props = ctrlPropsHelper.getProps(vlm);

            ctrlPropsHelper.fillProperties(LinStorObject.VOLUME, overrideProps, props, ApiConsts.FAIL_ACC_DENIED_VLM);
            ctrlPropsHelper.remove(props, deletePropKeys, deletePropNamespacesRef);

            ctrlTransactionHelper.commit();

            responseConverter.addWithDetail(
                responses,
                context,
                ctrlSatelliteUpdater.updateSatellites(vlm.getResource())
            );
            responseConverter.addWithOp(
                responses,
                context,
                ApiSuccessUtils.defaultModifiedEntry(vlm.getUuid(), getVlmDescriptionInline(vlm))
            );
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public static String getVlmDescription(Volume vlm)
    {
        return getVlmDescription(vlm.getResource(), vlm.getVolumeDefinition());
    }

    public static String getVlmDescription(Resource rsc, VolumeDefinition vlmDfn)
    {
        return getVlmDescription(
            rsc.getAssignedNode().getName().displayValue,
            rsc.getDefinition().getName().displayValue,
            vlmDfn.getVolumeNumber().value
        );
    }

    public static String getVlmDescription(NodeName nodeName, ResourceName rscName, VolumeNumber vlmNr)
    {
        return getVlmDescription(nodeName.getDisplayName(), rscName.displayValue, vlmNr.value);
    }

    public static String getVlmDescription(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "Volume with volume number '" + vlmNr + "' on resource '" + rscNameStr + "' on node '" +
            nodeNameStr + "'";
    }

    public static String getVlmDescriptionInline(Volume vlm)
    {
        return getVlmDescriptionInline(vlm.getResource(), vlm.getVolumeDefinition());
    }

    public static String getVlmDescriptionInline(Resource rsc, VolumeDefinition vlmDfn)
    {
        return getVlmDescriptionInline(
            rsc.getAssignedNode().getName().displayValue,
            rsc.getDefinition().getName().displayValue,
            vlmDfn.getVolumeNumber().value
        );
    }

    public static String getVlmDescriptionInline(String nodeNameStr, String rscNameStr, Integer vlmNr)
    {
        return "volume with volume number '" + vlmNr + "' on resource '" + rscNameStr + "' on node '" +
            nodeNameStr + "'";
    }

    static ResponseContext makeVlmContext(
        ApiOperation operation,
        String nodeNameStr,
        String rscNameStr,
        Integer vlmNr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        objRefs.put(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr));

        return new ResponseContext(
            operation,
            getVlmDescription(nodeNameStr, rscNameStr, vlmNr),
            getVlmDescriptionInline(nodeNameStr, rscNameStr, vlmNr),
            ApiConsts.MASK_VLM,
            objRefs
        );
    }
}
