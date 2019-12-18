package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.CoreModule;
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
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.locks.LockGuard;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.makeVlmDfnContext;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlVlmDfnModifyApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock rscDfnMapLock;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    CtrlVlmDfnModifyApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        rscDfnMapLock = rscDfnMapLockRef;
        peerAccCtx = peerAccCtxRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
        throws AccessDeniedException
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        ResourceName rscName = rscDfn.getName();

        Iterator<VolumeDefinition> vlmDfnIter = rscDfn.iterateVolumeDfn(apiCtx);
        while (vlmDfnIter.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIter.next();
            boolean resizing = vlmDfn.getFlags().isSet(apiCtx, VolumeDefinition.Flags.RESIZE);
            if (resizing)
            {
                fluxes.add(updateSatellites(rscName, vlmDfn.getVolumeNumber(), true));
            }
        }

        return fluxes;
    }

    public Flux<ApiCallRc> modifyVlmDfn(
        UUID vlmDfnUuid,
        String rscName,
        int vlmNr,
        Long size,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ResponseContext context = makeVlmDfnContext(
            ApiOperation.makeModifyOperation(),
            rscName,
            vlmNr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Modify volume definition",
                LockGuard.createDeferred(rscDfnMapLock.writeLock()),
                () -> modifyVlmDfnInTransaction(
                    vlmDfnUuid,
                    rscName,
                    vlmNr,
                    size,
                    overrideProps,
                    deletePropKeys
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> modifyVlmDfnInTransaction(
        UUID vlmDfnUuid,
        String rscNameStr,
        int vlmNrInt,
        Long size,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
        VolumeNumber vlmNr = LinstorParsingUtils.asVlmNr(vlmNrInt);
        VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfn(rscName, vlmNr, true);

        if (vlmDfnUuid != null && !vlmDfnUuid.equals(vlmDfn.getUuid()))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_UUID_VLM_DFN,
                "UUID check failed. Given UUID: " + vlmDfnUuid + ". Persisted UUID: " + vlmDfn.getUuid()
            ));
        }
        Props props = getVlmDfnProps(vlmDfn);
        Map<String, String> propsMap = props.map();

        ctrlPropsHelper.fillProperties(LinStorObject.VOLUME_DEFINITION, overrideProps,
            getVlmDfnProps(vlmDfn), ApiConsts.FAIL_ACC_DENIED_VLM_DFN);

        for (String delKey : deletePropKeys)
        {
            propsMap.remove(delKey);
        }

        boolean resize = size != null;
        if (resize)
        {
            long vlmDfnSize = getVlmDfnSize(vlmDfn);
            if (size >= vlmDfnSize)
            {
                setVlmDfnSize(vlmDfn, size);

                Iterator<Volume> vlmIter = iterateVolumes(vlmDfn);

                if (vlmIter.hasNext())
                {
                    markVlmDfnResize(vlmDfn);
                }

                while (vlmIter.hasNext())
                {
                    Volume vlm = vlmIter.next();

                    markVlmResize(vlm);
                }
            }
            else
            {
                if (!hasDeployedVolumes(vlmDfn))
                {
                    setVlmDfnSize(vlmDfn, size);
                }
                else
                {
                    throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_VLM_SIZE,
                        "Deployed volumes can only grow in size, not shrink."
                    ));
                }
            }
        }

        ctrlTransactionHelper.commit();

        ApiCallRc responses = ApiCallRcImpl.singletonApiCallRc(
            ApiSuccessUtils.defaultModifiedEntry(vlmDfn.getUuid(), getVlmDfnDescriptionInline(vlmDfn))
        );

        Flux<ApiCallRc> updateResponses = updateSatellites(rscName, vlmNr, resize);

        return Flux
            .just(responses)
            .concatWith(updateResponses);
    }

    // Restart from here when connection established and RESIZE flag set
    private Flux<ApiCallRc> updateSatellites(ResourceName rscName, VolumeNumber vlmNr, boolean resize)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Update for volume definition modification",
                LockGuard.createDeferred(rscDfnMapLock.readLock()),
                () -> updateSatellitesInScope(rscName, vlmNr, resize)
            );
    }

    private Flux<ApiCallRc> updateSatellitesInScope(ResourceName rscName, VolumeNumber vlmNr, boolean resize)
    {
        VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfn(rscName, vlmNr, false);

        Flux<ApiCallRc> flux;

        if (vlmDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            Flux<ApiCallRc> nextStep = Flux.empty();
            if (resize)
            {
                if (hasDrbd(vlmDfn))
                {
                    nextStep = resizeDrbd(rscName, vlmNr);
                }
                nextStep = nextStep.concatWith(finishResize(rscName, vlmNr));
            }
            flux = ctrlSatelliteUpdateCaller.updateSatellites(vlmDfn.getResourceDefinition(), nextStep)
                .transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        updateResponses,
                        rscName,
                        "Updated volume " + vlmNr + " of {1} on {0}"
                    )
                )
                .concatWith(nextStep)
                .onErrorResume(
                CtrlResponseUtils.DelayedApiRcException.class,
                ignored -> Flux.empty()
            );
        }

        return flux;
    }

    private boolean hasDrbd(VolumeDefinition vlmDfnRef)
    {
        boolean anyResourceHasDrbdLayer;
        try
        {
            anyResourceHasDrbdLayer = vlmDfnRef.streamVolumes(apiCtx).anyMatch(this::hasDrbd);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return anyResourceHasDrbdLayer;
    }

    private boolean hasDrbd(Volume vlm)
    {
        boolean hasDrbdLayer;
        try
        {
            hasDrbdLayer = !LayerUtils.getChildLayerDataByKind(
                vlm.getAbsResource().getLayerData(apiCtx),
                DeviceLayerKind.DRBD
            ).isEmpty();
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return hasDrbdLayer;
    }

    private Flux<ApiCallRc> resizeDrbd(ResourceName rscName, VolumeNumber vlmNr)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Resize DRBD",
                LockGuard.createDeferred(rscDfnMapLock.writeLock()),
                () -> resizeDrbdInTransaction(rscName, vlmNr)
            );
    }

    private Flux<ApiCallRc> resizeDrbdInTransaction(ResourceName rscName, VolumeNumber vlmNr)
    {
        VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfn(rscName, vlmNr, false);

        Flux<ApiCallRc> flux;

        if (vlmDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            Optional<Volume> drbdResizeVlm = streamVolumesPrivileged(vlmDfn)
                .filter(this::isDrbdDiskful)
                .findAny();
            drbdResizeVlm.ifPresent(this::markVlmDrbdResize);

            ctrlTransactionHelper.commit();

            flux = ctrlSatelliteUpdateCaller.updateSatellites(vlmDfn.getResourceDefinition(), Flux.empty())
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    updateResponses,
                    rscName,
                    getNodeNames(drbdResizeVlm),
                    "Resized DRBD resource {1} on {0}",
                    null
                )
            );
        }

        return flux;
    }

    private boolean isDrbdDiskful(Volume vlm)
    {
        boolean diskless;
        try
        {
            diskless = vlm.getAbsResource().isDrbdDiskless(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return !diskless;
    }

    private Flux<ApiCallRc> finishResize(ResourceName rscName, VolumeNumber vlmNr)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Clean up after resize",
                LockGuard.createDeferred(rscDfnMapLock.writeLock()),
                () -> finishResizeInTransaction(rscName, vlmNr)
            );
    }

    private Flux<ApiCallRc> finishResizeInTransaction(ResourceName rscName, VolumeNumber vlmNr)
    {
        VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfn(rscName, vlmNr, false);

        if (vlmDfn != null)
        {
            streamVolumesPrivileged(vlmDfn).forEach(
                vlm ->
                {
                    unmarkVlmDrbdResizePrivileged(vlm);
                    unmarkVlmResizePrivileged(vlm);
                }
            );

            unmarkVlmDfnResizePrivileged(vlmDfn);

            ctrlTransactionHelper.commit();
        }

        return Flux.empty();
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

    private void markVlmDfnResize(VolumeDefinition vlmDfn)
    {
        try
        {
            vlmDfn.getFlags().enableFlags(peerAccCtx.get(), VolumeDefinition.Flags.RESIZE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set volume definition resize flag",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void unmarkVlmDfnResizePrivileged(VolumeDefinition vlmDfn)
    {
        try
        {
            vlmDfn.getFlags().disableFlags(apiCtx, VolumeDefinition.Flags.RESIZE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void markVlmResize(Volume vlm)
    {
        try
        {
            vlm.getFlags().enableFlags(peerAccCtx.get(), Volume.Flags.RESIZE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set volume resize flag",
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void unmarkVlmResizePrivileged(Volume vlm)
    {
        try
        {
            vlm.getFlags().disableFlags(apiCtx, Volume.Flags.RESIZE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void markVlmDrbdResize(Volume vlm)
    {
        try
        {
            vlm.getFlags().enableFlags(peerAccCtx.get(), Volume.Flags.DRBD_RESIZE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set volume DRBD resize flag",
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void unmarkVlmDrbdResizePrivileged(Volume vlm)
    {
        try
        {
            vlm.getFlags().disableFlags(apiCtx, Volume.Flags.DRBD_RESIZE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private long getVlmDfnSize(VolumeDefinition vlmDfn)
    {
        long volumeSize;
        try
        {
            volumeSize = vlmDfn.getVolumeSize(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access Volume definition's size",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return volumeSize;
    }

    private void setVlmDfnSize(VolumeDefinition vlmDfn, Long size)
    {
        try
        {
            vlmDfn.setVolumeSize(peerAccCtx.get(), size);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "update Volume definition's size",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }

    }

    private boolean hasDeployedVolumes(VolumeDefinition vlmDfn)
    {
        boolean hasVolumes;
        try
        {
            hasVolumes = vlmDfn.iterateVolumes(peerAccCtx.get()).hasNext();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access volume definition",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return hasVolumes;
    }

    private Iterator<Volume> iterateVolumes(VolumeDefinition vlmDfn)
    {
        Iterator<Volume> volumeIterator;
        try
        {
            volumeIterator = vlmDfn.iterateVolumes(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "iterate volumes",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return volumeIterator;
    }

    private Stream<Volume> streamVolumesPrivileged(VolumeDefinition vlmDfn)
    {
        Stream<Volume> volumeStream;
        try
        {
            volumeStream = vlmDfn.streamVolumes(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return volumeStream;
    }

    private static Set<NodeName> getNodeNames(Optional<Volume> drbdResizeVlm)
    {
        return drbdResizeVlm.isPresent() ?
            Collections.singleton(drbdResizeVlm.get().getAbsResource().getNode().getName()) :
            Collections.emptySet();
    }
}
