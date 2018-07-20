package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeDefinitionDataControllerFactory;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.ConfigModule;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.OperationDescription;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotVlmDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

@Singleton
class CtrlSnapshotRestoreVlmDfnApiCallHandler extends CtrlVlmDfnCrtApiCallHandler
{
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;

    @Inject
    CtrlSnapshotRestoreVlmDfnApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtx,
        @Named(ConfigModule.CONFIG_STOR_POOL_NAME) String defaultStorPoolNameRef,
        CtrlObjectFactories objectFactories,
        VolumeDefinitionDataControllerFactory volumeDefinitionDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef
    )
    {
        super(
            errorReporterRef,
            apiCtx,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef,
            defaultStorPoolNameRef,
            volumeDefinitionDataFactoryRef
        );
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
    }

    public ApiCallRc restoreVlmDfn(String fromRscNameStr, String fromSnapshotNameStr, String toRscNameStr)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = new ResponseContext(
            peer.get(),
            new ApiOperation(ApiConsts.MASK_CRT, new OperationDescription("restore", "restoring")),
            getSnapshotRestoreVlmDfnDescription(toRscNameStr),
            getSnapshotRestoreVlmDfnDescriptionInline(toRscNameStr),
            ApiConsts.MASK_SNAPSHOT,
            Collections.emptyMap()
        );

        try
        {
            ResourceDefinitionData fromRscDfn = loadRscDfn(fromRscNameStr, true);

            SnapshotName fromSnapshotName = LinstorParsingUtils.asSnapshotName(fromSnapshotNameStr);
            SnapshotDefinition fromSnapshotDfn = loadSnapshotDfn(fromRscDfn, fromSnapshotName);

            ResourceDefinitionData toRscDfn = loadRscDfn(toRscNameStr, true);

            for (SnapshotVolumeDefinition snapshotVlmDfn : fromSnapshotDfn.getAllSnapshotVolumeDefinitions(peerAccCtx.get()))
            {
                VolumeDefinitionData vlmDfn = createVlmDfnData(
                    peerAccCtx.get(),
                    toRscDfn,
                    snapshotVlmDfn.getVolumeNumber(),
                    null,
                    snapshotVlmDfn.getVolumeSize(peerAccCtx.get()),
                    new VlmDfnFlags[] {}
                );

                boolean isEncrypted = snapshotVlmDfn.getFlags()
                    .isSet(peerAccCtx.get(), SnapshotVolumeDefinition.SnapshotVlmDfnFlags.ENCRYPTED);
                if (isEncrypted)
                {
                    Map<String, String> snapshotVlmDfnPropsMaps = getSnapshotVlmDfnProps(snapshotVlmDfn).map();
                    Map<String, String> vlmDfnPropsMap = getVlmDfnProps(vlmDfn).map();

                    String cryptPasswd = snapshotVlmDfnPropsMaps.get(ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD);
                    if (cryptPasswd == null)
                    {
                        throw new ImplementationError(
                            "Encrypted snapshot volume definition without crypt passwd found");
                    }

                    vlmDfn.getFlags().enableFlags(peerAccCtx.get(), VolumeDefinition.VlmDfnFlags.ENCRYPTED);
                    vlmDfnPropsMap.put(
                        ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD,
                        cryptPasswd
                    );
                }

                Iterator<Resource> rscIterator = getRscIterator(toRscDfn);
                while (rscIterator.hasNext())
                {
                    adjustRscVolumes(rscIterator.next());
                }
            }

            commit();

            responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(toRscDfn));

            responseConverter.addWithOp(responses, context, ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.DELETED,
                    firstLetterCaps(getSnapshotRestoreVlmDfnDescriptionInline(toRscNameStr)) + " restored " +
                        "from resource '" + fromRscNameStr + "', snapshot '" + fromSnapshotNameStr + "'."
                )
                .setDetails("Resource UUIDs: " +
                    toRscDfn.streamResource(peerAccCtx.get())
                        .map(Resource::getUuid)
                        .map(UUID::toString)
                        .collect(Collectors.joining(", ")))
                .build()
            );
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private Props getVlmDfnProps(VolumeDefinition vlmDfn)
    {
        Props props;
        try
        {
            props = vlmDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access the properties of " + getVlmDfnDescriptionInline(vlmDfn),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return props;
    }

    private Props getSnapshotVlmDfnProps(SnapshotVolumeDefinition snapshotVlmDfn)
    {
        Props props;
        try
        {
            props = snapshotVlmDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access the properties of " + getSnapshotVlmDfnDescriptionInline(snapshotVlmDfn),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_VLM_DFN
            );
        }
        return props;
    }

    private Iterator<Resource> getRscIterator(ResourceDefinition rscDfn)
    {
        Iterator<Resource> iterator;
        try
        {
            iterator = rscDfn.iterateResource(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access resources of resource definition " + rscDfn.getName().getDisplayName(),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return iterator;
    }

    private static String getSnapshotRestoreVlmDfnDescription(String toRscNameStr)
    {
        return "Volume definitions of resource: " + toRscNameStr;
    }

    private static String getSnapshotRestoreVlmDfnDescriptionInline(String toRscNameStr)
    {
        return "volume definitions of resource definition '" + toRscNameStr + "'";
    }
}
