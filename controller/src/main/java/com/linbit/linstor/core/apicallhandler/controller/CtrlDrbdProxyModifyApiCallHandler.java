package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.makeResourceDefinitionContext;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Singleton
public class CtrlDrbdProxyModifyApiCallHandler
{
    private static final String FULL_KEY_COMPRESSION_TYPE =
        ApiConsts.NAMESPC_DRBD_PROXY + "/" + ApiConsts.KEY_DRBD_PROXY_COMPRESSION_TYPE;

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
        Set<String> deletePropKeys,
        String compressionType,
        Map<String, String> compressionProps
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        boolean notifyStlts = false;
        ResponseContext context = makeResourceDefinitionContext(
            ApiOperation.makeModifyOperation(),
            rscNameStr
        );

        try
        {
            ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);
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

            Props props = ctrlPropsHelper.getProps(rscDfn);
            if (!overrideProps.isEmpty() || !deletePropKeys.isEmpty())
            {
                Map<String, String> map = props.map();

                notifyStlts = ctrlPropsHelper.fillProperties(responses, LinStorObject.DRBD_PROXY, overrideProps,
                    props, ApiConsts.FAIL_ACC_DENIED_RSC_DFN);

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
                    else
                    {
                        notifyStlts = true;
                    }
                }
            }

            if (compressionType != null)
            {
                notifyStlts = true;
                @Nullable Props namespace = props.getNamespace(ApiConsts.NAMESPC_DRBD_PROXY_COMPRESSION_OPTIONS);
                if (namespace != null)
                {
                    clearProps(namespace);
                }

                if (ApiConsts.VAL_DRBD_PROXY_COMPRESSION_NONE.equals(compressionType))
                {
                    props.map().remove(FULL_KEY_COMPRESSION_TYPE);
                }
                else
                {
                    LinStorObject linStorObject = LinStorObject.drbdProxyCompressionObject(compressionType);
                    if (linStorObject == null)
                    {
                        throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_DRBD_PROXY_COMPRESSION_TYPE,
                            "Unknown compression type '" + compressionType + "'"
                        ));
                    }

                    ctrlPropsHelper.fillProperties(
                        responses,
                        LinStorObject.RSC_DFN,
                        Collections.singletonMap(FULL_KEY_COMPRESSION_TYPE, compressionType),
                        props,
                        ApiConsts.FAIL_ACC_DENIED_RSC_DFN
                    );
                    ctrlPropsHelper.fillProperties(responses, linStorObject, compressionProps,
                        props, ApiConsts.FAIL_ACC_DENIED_RSC_DFN);
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
            if (notifyStlts)
            {
                responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(rscDfn));
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private void clearProps(Props props)
    {
        try
        {
            props.clear();
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "reset DRBD Proxy compression properties",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }
}
