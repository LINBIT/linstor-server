package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.makeResourceDefinitionContext;

@Singleton
public class CtrlDrbdProxyModifyApiCallHandler
{
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;

    @Inject
    public CtrlDrbdProxyModifyApiCallHandler(
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

    public ApiCallRc modifyDrbdProxy(
        UUID rscDfnUuid,
        String rscNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeResourceDefinitionContext(
            ApiOperation.makeModifyOperation(),
            rscNameStr
        );

        try
        {
            ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
            ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);
            if (rscDfnUuid != null && !rscDfnUuid.equals(rscDfn.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_UUID_RSC_DFN,
                        "UUID-check failed"
                    )
                    .setDetails("local UUID: " + rscDfn.getUuid().toString() +
                        ", received UUID: " + rscDfnUuid.toString())
                    .build()
                );
            }
            if (!overrideProps.isEmpty() || !deletePropKeys.isEmpty())
            {
                Map<String, String> map = ctrlPropsHelper.getProps(rscDfn).map();

                ctrlPropsHelper.fillProperties(LinStorObject.DRBD_PROXY, overrideProps,
                    ctrlPropsHelper.getProps(rscDfn), ApiConsts.FAIL_ACC_DENIED_RSC_DFN);

                for (String delKey : deletePropKeys)
                {
                    String oldValue = map.remove(delKey);
                    if (oldValue == null)
                    {
                        responseConverter.addWithDetail(responses, context, ApiCallRcImpl.simpleEntry(
                            ApiConsts.WARN_DEL_UNSET_PROP,
                            "Could not delete property '" + delKey + "' as it did not exist. " +
                                                "This operation had no effect."
                        ));
                    }
                }
            }

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(
                responses,
                context,
                ApiSuccessUtils.defaultModifiedEntry(
                    rscDfn.getUuid(),
                    "proxy options for " + getRscDfnDescriptionInline(rscDfn)
                )
            );
            responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(rscDfn));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }
}
