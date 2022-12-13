package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.DivergentUuidsException;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionApi;
import com.linbit.linstor.core.apis.SnapshotVolumeApi;
import com.linbit.linstor.core.apis.SnapshotVolumeDefinitionApi;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.SnapshotSatelliteFactory;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.SnapshotVolumeSatelliteFactory;
import com.linbit.linstor.core.objects.merger.StltLayerSnapDataMerger;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Singleton
class StltSnapshotApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final CoreModule.ResourceGroupMap rscGrpMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final ControllerPeerConnector controllerPeerConnector;
    private final ResourceDefinitionSatelliteFactory resourceDefinitionFactory;
    private final SnapshotDefinitionSatelliteFactory snapshotDefinitionFactory;
    private final SnapshotVolumeDefinitionSatelliteFactory snapshotVolumeDefinitionFactory;
    private final SnapshotSatelliteFactory snapshotFactory;
    private final SnapshotVolumeSatelliteFactory snapshotVolumeFactory;
    private final StltRscGrpApiCallHelper rscGrpApiCallHelper;
    private final StltLayerSnapDataMerger layerSnapDataMerger;
    private final Provider<TransactionMgr> transMgrProvider;
    private final BackupShippingMgr backupShippingMgr;

    @Inject
    StltSnapshotApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        CoreModule.ResourceGroupMap rscGrpMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        ResourceDefinitionSatelliteFactory resourceDefinitionFactoryRef,
        SnapshotDefinitionSatelliteFactory snapshotDefinitionFactoryRef,
        SnapshotVolumeDefinitionSatelliteFactory snapshotVolumeDefinitionFactoryRef,
        SnapshotSatelliteFactory snapshotFactoryRef,
        SnapshotVolumeSatelliteFactory snapshotVolumeFactoryRef,
        StltRscGrpApiCallHelper stltGrpApiCallHelperRef,
        StltLayerSnapDataMerger layerSnapDataMergerRef,
        Provider<TransactionMgr> transMgrProviderRef,
        BackupShippingMgr backupShippingMgrRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        rscGrpMap = rscGrpMapRef;
        rscDfnMap = rscDfnMapRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        resourceDefinitionFactory = resourceDefinitionFactoryRef;
        snapshotDefinitionFactory = snapshotDefinitionFactoryRef;
        snapshotVolumeDefinitionFactory = snapshotVolumeDefinitionFactoryRef;
        snapshotFactory = snapshotFactoryRef;
        snapshotVolumeFactory = snapshotVolumeFactoryRef;
        rscGrpApiCallHelper = stltGrpApiCallHelperRef;
        layerSnapDataMerger = layerSnapDataMergerRef;
        transMgrProvider = transMgrProviderRef;
        backupShippingMgr = backupShippingMgrRef;
    }

    public void applyChanges(SnapshotPojo snapshotRaw)
    {
        try
        {
            ResourceDefinition rscDfn = mergeResourceDefinition(snapshotRaw.getSnaphotDfn().getRscDfn());

            SnapshotDefinition snapshotDfn = mergeSnapshotDefinition(snapshotRaw.getSnaphotDfn(), rscDfn);

            mergeSnapshot(snapshotRaw, snapshotDfn);

            transMgrProvider.get().commit();

            errorReporter.logInfo(
                "Snapshot '%s' of resource '%s' registered.",
                snapshotDfn.getName().displayValue,
                rscDfn.getName().displayValue
            );

            rscDfnMap.put(rscDfn.getName(), rscDfn);

            deviceManager.snapshotUpdateApplied(Collections.singleton(
                snapshotDfn.getSnapDfnKey()
            ));
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
        }
    }

    private ResourceDefinition mergeResourceDefinition(ResourceDefinitionApi rscDfnApi)
        throws InvalidNameException, DivergentUuidsException, AccessDeniedException, DatabaseException
    {
        ResourceName rscName = new ResourceName(rscDfnApi.getResourceName());

        ResourceDefinition.Flags[] rscDfnFlags = ResourceDefinition.Flags.restoreFlags(rscDfnApi.getFlags());

        ResourceGroup rscGrp = rscGrpApiCallHelper.mergeResourceGroup(rscDfnApi.getResourceGroup());

        ResourceDefinition rscDfn = rscDfnMap.get(rscName);
        if (rscDfn == null)
        {
            rscDfn = resourceDefinitionFactory.getInstanceSatellite(
                apiCtx,
                rscDfnApi.getUuid(),
                rscGrp,
                rscName,
                rscDfnFlags
            );

            checkUuid(rscDfn, rscDfnApi);

            rscDfnMap.put(rscName, rscDfn);
        }
        Props rscDfnProps = rscDfn.getProps(apiCtx);
        rscDfnProps.map().putAll(rscDfnApi.getProps());
        rscDfnProps.keySet().retainAll(rscDfnApi.getProps().keySet());
        rscDfn.getFlags().resetFlagsTo(apiCtx, rscDfnFlags);
        return rscDfn;
    }

    private SnapshotDefinition mergeSnapshotDefinition(
        SnapshotDefinitionApi snapshotDfnApi,
        ResourceDefinition rscDfn
    )
        throws AccessDeniedException, DivergentUuidsException, InvalidNameException, ValueOutOfRangeException,
            DatabaseException
    {
        SnapshotName snapshotName = new SnapshotName(snapshotDfnApi.getSnapshotName());

        SnapshotDefinition snapshotDfn = rscDfn.getSnapshotDfn(apiCtx, snapshotName);
        if (snapshotDfn == null)
        {
            snapshotDfn = snapshotDefinitionFactory.getInstanceSatellite(
                apiCtx,
                snapshotDfnApi.getUuid(),
                rscDfn,
                snapshotName,
                new SnapshotDefinition.Flags[]{}
            );

            rscDfn.addSnapshotDfn(apiCtx, snapshotDfn);
        }
        checkUuid(snapshotDfn, snapshotDfnApi);

        Props snapshotDfnProps = snapshotDfn.getProps(apiCtx);
        snapshotDfnProps.map().putAll(snapshotDfnApi.getProps());
        snapshotDfnProps.keySet().retainAll(snapshotDfnApi.getProps().keySet());

        snapshotDfn.getFlags().resetFlagsTo(apiCtx, SnapshotDefinition.Flags.restoreFlags(snapshotDfnApi.getFlags()));
        errorReporter.logTrace(
            "resetting flags of local snapdfn (%s) to %s",
            snapshotDfn,
            FlagsHelper.toStringList(SnapshotDefinition.Flags.class, snapshotDfnApi.getFlags())
        );

        // Merge satellite volume definitions
        Set<VolumeNumber> oldVolumeNumbers = snapshotDfn.getAllSnapshotVolumeDefinitions(apiCtx).stream()
            .map(SnapshotVolumeDefinition::getVolumeNumber)
            .collect(Collectors.toCollection(HashSet::new));

        for (SnapshotVolumeDefinitionApi snapshotVlmDfnApi : snapshotDfnApi.getSnapshotVlmDfnList())
        {
            VolumeNumber volumeNumber = new VolumeNumber(snapshotVlmDfnApi.getVolumeNr());
            oldVolumeNumbers.remove(volumeNumber);

            SnapshotVolumeDefinition.Flags[] snapshotVlmDfnFlags = SnapshotVolumeDefinition.Flags.restoreFlags(
                snapshotVlmDfnApi.getFlags()
            );

            SnapshotVolumeDefinition snapshotVolumeDefinition =
                snapshotDfn.getSnapshotVolumeDefinition(apiCtx, volumeNumber);
            if (snapshotVolumeDefinition == null)
            {
                snapshotVolumeDefinition = snapshotVolumeDefinitionFactory.getInstanceSatellite(
                    apiCtx,
                    snapshotVlmDfnApi.getUuid(),
                    snapshotDfn,
                    rscDfn.getVolumeDfn(apiCtx, volumeNumber),
                    volumeNumber,
                    snapshotVlmDfnApi.getSize(),
                    snapshotVlmDfnFlags
                );
            }

            Props snapVlmDfnProps = snapshotVolumeDefinition.getProps(apiCtx);
            snapVlmDfnProps.map().putAll(snapshotVlmDfnApi.getPropsMap());
            snapVlmDfnProps.keySet().retainAll(snapshotVlmDfnApi.getPropsMap().keySet());

            snapshotVolumeDefinition.getFlags().resetFlagsTo(apiCtx, snapshotVlmDfnFlags);
        }

        for (VolumeNumber oldVolumeNumber : oldVolumeNumbers)
        {
            snapshotDfn.removeSnapshotVolumeDefinition(apiCtx, oldVolumeNumber);
        }

        return snapshotDfn;
    }

    private void mergeSnapshot(SnapshotPojo snapshotRaw, SnapshotDefinition snapshotDfn)
        throws DivergentUuidsException, AccessDeniedException, ValueOutOfRangeException, InvalidNameException,
            DatabaseException
    {
        Node localNode = controllerPeerConnector.getLocalNode();
        Snapshot snapshot = snapshotDfn.getSnapshot(apiCtx, localNode.getName());
        Snapshot.Flags[] snapshotFlags = Snapshot.Flags.restoreFlags(snapshotRaw.getFlags());
        if (snapshot == null)
        {
            snapshot = snapshotFactory.getInstanceSatellite(
                apiCtx,
                snapshotRaw.getSnapshotUuid(),
                localNode,
                snapshotDfn,
                snapshotFlags
            );
        }
        checkUuid(snapshot, snapshotRaw);
        snapshot.getFlags().resetFlagsTo(apiCtx, snapshotFlags);
        errorReporter.logTrace(
            "resetting flags of local snapshot (%s) to %s, suspendIO: %b, takeSnapshot: %b",
            snapshot,
            FlagsHelper.toStringList(Snapshot.Flags.class, snapshotRaw.getFlags()),
            snapshotRaw.getSuspendResource(),
            snapshotRaw.getTakeSnapshot()
        );
        snapshot.setSuspendResource(apiCtx, snapshotRaw.getSuspendResource());
        snapshot.setTakeSnapshot(apiCtx, snapshotRaw.getTakeSnapshot());

        Props snapProps = snapshot.getProps(apiCtx);
        snapProps.map().putAll(snapshotRaw.getPropsMap());
        snapProps.keySet().retainAll(snapshotRaw.getPropsMap().keySet());

        for (SnapshotVolumeApi snapshotVlmApi : snapshotRaw.getSnapshotVlmList())
        {
            mergeSnapshotVolume(snapshotVlmApi, snapshot);
        }

        layerSnapDataMerger.mergeLayerData(snapshot, snapshotRaw.getLayerData(), false);
    }

    private void mergeSnapshotVolume(SnapshotVolumeApi snapshotVlmApi, Snapshot snapshot)
        throws ValueOutOfRangeException, DivergentUuidsException, InvalidNameException, AccessDeniedException
    {
        VolumeNumber volumeNumber = new VolumeNumber(snapshotVlmApi.getSnapshotVlmNr());
        SnapshotVolume snapshotVolume = snapshot.getVolume(volumeNumber);
        if (snapshotVolume == null)
        {
            snapshotVolume = snapshotVolumeFactory.getInstanceSatellite(
                apiCtx,
                snapshotVlmApi.getSnapshotVlmUuid(),
                snapshot,
                snapshot.getSnapshotDefinition().getSnapshotVolumeDefinition(apiCtx, volumeNumber)
            );

            snapshot.putVolume(apiCtx, snapshotVolume);
        }

        Props snapVlmProps = snapshotVolume.getProps(apiCtx);
        snapVlmProps.map().putAll(snapshotVlmApi.getPropsMap());
        snapVlmProps.keySet().retainAll(snapshotVlmApi.getPropsMap().keySet());

        checkUuid(snapshotVolume, snapshotVlmApi);
    }

    public void applyEndedSnapshot(String rscNameStr, String snapshotNameStr)
    {
        try
        {
            ResourceName rscName = new ResourceName(rscNameStr);
            SnapshotName snapshotName = new SnapshotName(snapshotNameStr);

            ResourceDefinition rscDfn = rscDfnMap.get(rscName);
            if (rscDfn != null)
            {
                SnapshotDefinition snapDfn = rscDfn.getSnapshotDfn(apiCtx, snapshotName);
                if (snapDfn != null)
                {
                    for (Snapshot snap : snapDfn.getAllSnapshots(apiCtx))
                    {
                        backupShippingMgr.snapshotDeleted(snap);
                        snap.delete(apiCtx);
                    }
                    snapDfn.delete(apiCtx);

                    if (rscDfn.getResourceCount() == 0 && rscDfn.getSnapshotDfns(apiCtx).isEmpty())
                    {
                        ResourceDefinition removedRscDfn = rscDfnMap.remove(rscName);
                        if (removedRscDfn != null)
                        {
                            ResourceGroup rscGrp = removedRscDfn.getResourceGroup();
                            removedRscDfn.delete(apiCtx);
                            if (!rscGrp.hasResourceDefinitions(apiCtx))
                            {
                                rscGrpMap.remove(rscGrp.getName());
                                rscGrp.delete(apiCtx);
                            }
                        }
                    }
                }
                transMgrProvider.get().commit();
            }

            errorReporter.logInfo(
                "Snapshot '%s' ended.",
                snapshotName
            );

            deviceManager.snapshotUpdateApplied(
                Collections.singleton(new SnapshotDefinition.Key(rscName, snapshotName))
            );
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
        }
    }

    private void checkUuid(ResourceDefinition rscDfn, ResourceDefinitionApi rscDfnApi)
        throws DivergentUuidsException
    {
        checkUuid(
            rscDfn.getUuid(),
            rscDfnApi.getUuid(),
            "ResourceDefinition",
            rscDfn.getName().displayValue,
            rscDfnApi.getResourceName()
        );
    }

    private void checkUuid(SnapshotDefinition snapshotDfn, SnapshotDefinitionApi snapshotDfnApi)
        throws DivergentUuidsException
    {
        checkUuid(
            snapshotDfn.getUuid(),
            snapshotDfnApi.getUuid(),
            "SnapshotDefinition",
            snapshotDfn.getName().displayValue,
            snapshotDfnApi.getSnapshotName()
        );
    }

    private void checkUuid(Snapshot snapshot, SnapshotPojo snapshotRaw)
        throws DivergentUuidsException
    {
        checkUuid(
            snapshot.getUuid(),
            snapshotRaw.getSnapshotUuid(),
            "Snapshot",
            snapshot.getSnapshotName().displayValue,
            snapshotRaw.getSnaphotDfn().getSnapshotName()
        );
    }

    private void checkUuid(SnapshotVolume snapshotVolume, SnapshotVolumeApi snapshotVlmApi)
        throws DivergentUuidsException
    {
        checkUuid(
            snapshotVolume.getUuid(),
            snapshotVlmApi.getSnapshotVlmUuid(),
            "SnapshotVolume",
            String.valueOf(snapshotVolume.getVolumeNumber()),
            String.valueOf(snapshotVlmApi.getSnapshotVlmNr())
        );
    }

    private void checkUuid(UUID localUuid, UUID remoteUuid, String type, String localName, String remoteName)
        throws DivergentUuidsException
    {
        if (!localUuid.equals(remoteUuid))
        {
            throw new DivergentUuidsException(
                type,
                localName,
                remoteName,
                localUuid,
                remoteUuid
            );
        }
    }
}
