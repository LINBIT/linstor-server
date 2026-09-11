package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.utils.PairNonNull;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlResyncAfterHelper
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final LockGuardFactory lockGuardFactory;
    private final SystemConfRepository sysCfgRepo;
    private final StorPoolDefinitionRepository storPoolDfnRepo;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;

    private static final String DRBD_RESYNC_AFTER_KEY = ApiConsts.NAMESPC_DRBD_DISK_OPTIONS + "/resync-after";

    @Inject
    public CtrlResyncAfterHelper(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        LockGuardFactory lockGuardFactoryRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        SystemConfRepository systemConfRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef)
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        lockGuardFactory = lockGuardFactoryRef;
        sysCfgRepo = systemConfRepositoryRef;
        storPoolDfnRepo = storPoolDefinitionRepositoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
    }

    public Flux<ApiCallRc> fluxManage()
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Update resync-after entries",
                lockGuardFactory.create()
                    .write(LockGuardFactory.LockObj.RSC_DFN_MAP, LockGuardFactory.LockObj.NODES_MAP).buildDeferred(),
                this::updateResyncAfterInTx
            );
    }

    public PairNonNull<ApiCallRc, Set<Resource>> manage()
    {
        return updateResyncAfter();
    }

    public PairNonNull<ApiCallRc, Set<Resource>> clearAllResyncAfterProps()
    {
        Set<Resource> changed = new HashSet<>();
        final ApiCallRcImpl rcs = new ApiCallRcImpl();
        try
        {
            for (final Map.Entry<StorPoolName, StorPoolDefinition> entry : storPoolDfnRepo
                .getMapForView().entrySet())
            {
                Iterator<StorPool> itStorPool = entry.getValue().iterateStorPools();
                while (itStorPool.hasNext())
                {
                    final StorPool sp = itStorPool.next();
                    for (final VlmProviderObject<Resource> vol : sp.getVolumes())
                    {
                        Props props = ((Volume) vol.getVolume()).getProps();
                        if (props.contains(Collections.singletonList(DRBD_RESYNC_AFTER_KEY)))
                        {
                            changed.add(vol.getVolume().getAbsResource());
                            props.removeProp(DRBD_RESYNC_AFTER_KEY);
                        }
                    }
                }
            }
            final String infoMsg = "Removed all DRBD resync-after properties";
            rcs.addEntry(infoMsg, ApiConsts.MASK_INFO);
            errorReporter.logInfo(infoMsg);
        }
        catch (DatabaseException dbExc)
        {
            errorReporter.reportError(dbExc);
            throw new ApiDatabaseException(dbExc);
        }

        return new PairNonNull<>(rcs, changed);
    }

    private Flux<ApiCallRc> updateResyncAfterInTx()
    {
        PairNonNull<ApiCallRc, Set<Resource>> result = updateResyncAfter();
        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> flux = Flux.just(result.objA);
        for (Resource rsc : result.objB)
        {
            flux = flux.concatWith(ctrlSatelliteUpdateCaller.updateSatellites(rsc, Flux.empty())
                .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2()));
        }
        return flux;
    }

    /**
     * Updates all resync-after options for drbd resources/volumes
     *
     * @return A Pair with ApiCallRc with the update message if properties have been changed
     *         and a set of involved resources, which is useful to notify satellites.
     * @throws ApiException             If an invalid value would be set
     * @throws ApiDatabaseException     if setProp fails
     */
    @SuppressWarnings("checkstyle:IllegalToken")
    private PairNonNull<ApiCallRc, Set<Resource>> updateResyncAfter()
    {
        final ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        final HashSet<Resource> modifiedRscs = new HashSet<>();

        try
        {
            final ReadOnlyProps ctrlProps = sysCfgRepo.getCtrlConfForView();
            final String disableAuto = ctrlProps.getProp(ApiConsts.KEY_DRBD_DISABLE_AUTO_RESYNC_AFTER,
                ApiConsts.NAMESPC_DRBD_OPTIONS);
            if (!"true".equalsIgnoreCase(disableAuto))
            {
                for (final Map.Entry<StorPoolName, StorPoolDefinition> entry : storPoolDfnRepo
                    .getMapForView().entrySet())
                {
                    final Iterator<StorPool> itStorPool = entry.getValue().iterateStorPools();
                    while (itStorPool.hasNext())
                    {
                        final StorPool sp = itStorPool.next();
                        if (sp.getDeviceProviderKind() == DeviceProviderKind.DISKLESS)
                        {
                            // ignore diskless pools
                            continue;
                        }

                        final TreeMap<MinorNumber, VlmProviderObject<Resource>> spVolsSorted = new TreeMap<>();
                        for (final VlmProviderObject<Resource> vol : sp.getVolumes())
                        {
                            final Resource rsc = vol.getRscLayerObject().getAbsResource();
                            final Set<AbsRscLayerObject<Resource>> drbdRscSet = LayerRscUtils
                                .getRscDataByLayer(rsc.getLayerData(), DeviceLayerKind.DRBD);
                            if (!drbdRscSet.isEmpty())
                            {
                                final DrbdRscData<Resource> drbdRsc =
                                    (DrbdRscData<Resource>) drbdRscSet.iterator().next();
                                final DrbdVlmDfnData<Resource> vlmDfnData = drbdRsc.getVlmLayerObjects()
                                    .get(vol.getVlmNr()).getVlmDfnLayerObject();
                                if (vlmDfnData != null &&
                                    !rsc.isDiskless() &&
                                    !rsc.isDeleted() &&
                                    !vlmDfnData.getVolumeDefinition().isDeleted() &&
                                    vlmDfnData.getVolumeDefinition().getFlags().isUnset(VolumeDefinition.Flags.DELETE) &&
                                    rsc.getStateFlags().isUnset(Resource.Flags.DELETE))
                                {
                                    spVolsSorted.put(vlmDfnData.getMinorNr(), vol);
                                }
                            }
                        }

                        String rscNameBefore = null;
                        for (VlmProviderObject<Resource> vol : spVolsSorted.values())
                        {
                            final Props props = ((Volume) vol.getVolume()).getProps();
                            if (rscNameBefore == null)
                            {
                                if (props.removeProp(DRBD_RESYNC_AFTER_KEY) != null)
                                {
                                    modifiedRscs.add(vol.getVolume().getAbsResource());
                                }
                            }
                            else
                            {
                                String resyncAfterValue = props.getProp(DRBD_RESYNC_AFTER_KEY);
                                if (!rscNameBefore.equals(resyncAfterValue))
                                {
                                    modifiedRscs.add(vol.getVolume().getAbsResource());
                                    props.setProp(
                                        DRBD_RESYNC_AFTER_KEY,
                                        rscNameBefore);
                                }
                            }
                            rscNameBefore = vol.getRscLayerObject().getSuffixedResourceName() + "/" + vol.getVlmNr();
                        }
                    }
                }

                final String infoMsg = "Updated " + modifiedRscs.size() + " resync-after entries.";
                apiCallRc.addEntry(infoMsg, ApiConsts.MASK_INFO);
                errorReporter.logInfo(infoMsg);
            }
        }
        catch (DatabaseException dbExc)
        {
            errorReporter.reportError(dbExc);
            throw new ApiDatabaseException(dbExc);
        }
        catch (InvalidValueException invValExc)
        {
            errorReporter.reportError(invValExc);
            throw new ApiException(invValExc);
        }

        return new PairNonNull<>(apiCallRc, modifiedRscs);
    }
}
