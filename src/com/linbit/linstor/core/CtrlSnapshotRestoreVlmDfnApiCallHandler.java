package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeDefinitionDataControllerFactory;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.stream.Collectors;

class CtrlSnapshotRestoreVlmDfnApiCallHandler extends CtrlVlmDfnCrtApiCallHandler
{
    private String currentFromRscName;
    private String currentFromSnapshotName;
    private String currentToRscName;

    @Inject
    CtrlSnapshotRestoreVlmDfnApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        @ApiContext AccessContext apiCtx,
        @Named(ConfigModule.CONFIG_STOR_POOL_NAME) String defaultStorPoolNameRef,
        CtrlObjectFactories objectFactories,
        VolumeDefinitionDataControllerFactory volumeDefinitionDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef
    )
    {
        super(
            errorReporterRef,
            apiCtx,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef,
            defaultStorPoolNameRef,
            volumeDefinitionDataFactoryRef
        );
    }

    public ApiCallRc restoreVlmDfn(String fromRscNameStr, String fromSnapshotNameStr, String toRscNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.CREATE,
                apiCallRc,
                fromRscNameStr,
                fromSnapshotNameStr,
                toRscNameStr
            )
        )
        {
            ResourceDefinitionData fromRscDfn = loadRscDfn(fromRscNameStr, true);

            SnapshotName fromSnapshotName = asSnapshotName(fromSnapshotNameStr);
            SnapshotDefinition fromSnapshotDfn = loadSnapshotDfn(fromRscDfn, fromSnapshotName);

            ResourceDefinitionData toRscDfn = loadRscDfn(toRscNameStr, true);

            for (SnapshotVolumeDefinition snapshotVolumeDefinition : fromSnapshotDfn.getAllSnapshotVolumeDefinitions())
            {
                VolumeDefinitionData vlmDfn = createVlmDfnData(
                    peerAccCtx,
                    toRscDfn,
                    snapshotVolumeDefinition.getVolumeNumber(),
                    null,
                    snapshotVolumeDefinition.getVolumeSize(peerAccCtx),
                    new VlmDfnFlags[] {}
                );

                Iterator<Resource> rscIterator = getRscIterator(toRscDfn);
                while (rscIterator.hasNext())
                {
                    adjustRscVolumes(rscIterator.next());
                }
            }

            commit();

            updateSatellites(toRscDfn);

            reportSuccess(
                getObjectDescriptionInlineFirstLetterCaps() + " restored " +
                    "from resource '" + fromRscNameStr + "', snapshot '" + fromSnapshotNameStr + "'.",
                "Resource UUIDs: " +
                    toRscDfn.streamResource(peerAccCtx)
                        .map(Resource::getUuid)
                        .map(UUID::toString)
                        .collect(Collectors.joining(", "))
            );
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
                getObjectDescriptionInline(toRscNameStr),
                new HashMap(),
                new HashMap(),
                apiCallRc
            );
        }

        return apiCallRc;
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
            throw asAccDeniedExc(
                accDeniedExc,
                "accessing resources of resource definition " + rscDfn.getName().getDisplayName(),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return iterator;
    }

    private AbsApiCallHandler setContext(
        ApiCallType apiCallType,
        ApiCallRcImpl apiCallRc,
        String fromRscNameStr,
        String fromSnapshotNameStr,
        String toRscNameStr
    )
    {
        super.setContext(
            apiCallType,
            apiCallRc,
            true, // autoClose
            new HashMap(),
            new HashMap()
        );

        currentFromRscName = fromRscNameStr;
        currentFromSnapshotName = fromSnapshotNameStr;
        currentToRscName = toRscNameStr;

        return this;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Volume definitions of resource: " + currentToRscName;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentToRscName);
    }

    public static String getObjectDescriptionInline(String toRscNameStr)
    {
        return "volume definitions of resource definition '" + toRscNameStr + "'";
    }
}
