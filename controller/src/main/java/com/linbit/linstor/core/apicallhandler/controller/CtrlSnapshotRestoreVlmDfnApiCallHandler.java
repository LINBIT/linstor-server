package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.OperationDescription;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Singleton
class CtrlSnapshotRestoreVlmDfnApiCallHandler
{
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelper;
    private final CtrlVlmCrtApiHelper ctrlVlmCrtApiHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlPropsHelper propsHelper;

    @Inject
    CtrlSnapshotRestoreVlmDfnApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelperRef,
        CtrlVlmCrtApiHelper ctrlVlmCrtApiHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlPropsHelper propsHelperRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlVlmDfnCrtApiHelper = ctrlVlmDfnCrtApiHelperRef;
        ctrlVlmCrtApiHelper = ctrlVlmCrtApiHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        backupInfoMgr = backupInfoMgrRef;
        propsHelper = propsHelperRef;
    }

    public ApiCallRc restoreVlmDfn(
        String fromRscNameStr,
        String fromSnapshotNameStr,
        String toRscNameStr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = new ResponseContext(
            new ApiOperation(ApiConsts.MASK_CRT, new OperationDescription("restore", "restoring")),
            getSnapshotRestoreVlmDfnDescription(toRscNameStr),
            getSnapshotRestoreVlmDfnDescriptionInline(toRscNameStr),
            ApiConsts.MASK_SNAPSHOT,
            Collections.emptyMap()
        );

        try
        {
            ResourceDefinition fromRscDfn = ctrlApiDataLoader.loadRscDfn(fromRscNameStr, true);

            SnapshotName fromSnapshotName = LinstorParsingUtils.asSnapshotName(fromSnapshotNameStr);
            SnapshotDefinition fromSnapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(fromRscDfn, fromSnapshotName, true);

            ResourceDefinition toRscDfn = ctrlApiDataLoader.loadRscDfn(toRscNameStr, true);

            if (backupInfoMgr.restoreContainsRscDfn(toRscDfn))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_IN_USE,
                        toRscNameStr + " is currently being restored from a backup. " +
                            "Please wait until the restore is finished"
                    )
                );
            }

            for (SnapshotVolumeDefinition snapshotVlmDfn :
                fromSnapshotDfn.getAllSnapshotVolumeDefinitions(peerAccCtx.get()))
            {
                VolumeDefinition vlmDfn = ctrlVlmDfnCrtApiHelper.createVlmDfnData(
                    peerAccCtx.get(),
                    toRscDfn,
                    snapshotVlmDfn.getVolumeNumber(),
                    null,
                    snapshotVlmDfn.getVolumeSize(peerAccCtx.get()),
                    new VolumeDefinition.Flags[] {}
                );

                Map<String, String> snapVlmDfnVlmDfnProps = propsHelper.getProps(snapshotVlmDfn, true).map();
                Map<String, String> vlmDfnProps = getVlmDfnProps(vlmDfn).map();

                boolean isEncrypted = snapshotVlmDfn.getFlags()
                    .isSet(peerAccCtx.get(), SnapshotVolumeDefinition.Flags.ENCRYPTED);
                if (isEncrypted)
                {
                    String cryptPasswd = snapVlmDfnVlmDfnProps.get(ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD);
                    if (cryptPasswd == null)
                    {
                        throw new ImplementationError(
                            "Encrypted snapshot volume definition without crypt passwd found");
                    }

                    vlmDfn.getFlags().enableFlags(peerAccCtx.get(), VolumeDefinition.Flags.ENCRYPTED);
                    vlmDfnProps.put(
                        ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD,
                        cryptPasswd
                    );
                }

                copyProp(snapVlmDfnVlmDfnProps, vlmDfnProps, ApiConsts.KEY_DRBD_CURRENT_GI);

                Iterator<Resource> rscIterator = getRscIterator(toRscDfn);
                while (rscIterator.hasNext())
                {
                    ApiCallRcWith<Volume> respWithVlm = ctrlVlmCrtApiHelper.createVolumeResolvingStorPool(
                        rscIterator.next(),
                        vlmDfn,
                        null,
                        Collections.emptyMap()
                    );
                    respWithVlm.extractApiCallRc(responses);
                }
            }

            ctrlTransactionHelper.commit();

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

    private void copyProp(
        Map<String, String> snapshotVlmDfnProps,
        Map<String, String> vlmDfnProps,
        String propKey
    )
    {
        String propValue = snapshotVlmDfnProps.get(propKey);
        if (propValue != null)
        {
            vlmDfnProps.put(propKey, propValue);
        }
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
