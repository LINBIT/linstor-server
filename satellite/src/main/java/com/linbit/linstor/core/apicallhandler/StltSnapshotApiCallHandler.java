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
import com.linbit.linstor.core.identifier.ResourceGroupName;
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
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.PropsUtils;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Singleton
public class StltSnapshotApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final CoreModule.ResourceGroupMap rscGrpMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final ControllerPeerConnector controllerPeerConnector;
    private final ResourceDefinitionSatelliteFactory resourceDefinitionFactory;
    private final SnapshotDefinitionSatelliteFactory snapDfnFactory;
    private final SnapshotVolumeDefinitionSatelliteFactory snapVlmDfnFactory;
    private final SnapshotSatelliteFactory snapshotFactory;
    private final SnapshotVolumeSatelliteFactory snapVlmFactory;
    private final StltRscGrpApiCallHelper rscGrpApiCallHelper;
    private final StltLayerSnapDataMerger layerSnapDataMerger;
    private final Provider<TransactionMgr> transMgrProvider;
    private final BackupShippingMgr backupShippingMgr;
    private final SnapshotShippingService snapShipService;

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
        BackupShippingMgr backupShippingMgrRef,
        SnapshotShippingService snapShipServiceRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        rscGrpMap = rscGrpMapRef;
        rscDfnMap = rscDfnMapRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        resourceDefinitionFactory = resourceDefinitionFactoryRef;
        snapDfnFactory = snapshotDefinitionFactoryRef;
        snapVlmDfnFactory = snapshotVolumeDefinitionFactoryRef;
        snapshotFactory = snapshotFactoryRef;
        snapVlmFactory = snapshotVolumeFactoryRef;
        rscGrpApiCallHelper = stltGrpApiCallHelperRef;
        layerSnapDataMerger = layerSnapDataMergerRef;
        transMgrProvider = transMgrProviderRef;
        backupShippingMgr = backupShippingMgrRef;
        snapShipService = snapShipServiceRef;
    }

    public void applyChanges(SnapshotPojo snapshotRaw)
    {
        try
        {
            ResourceDefinition rscDfn = mergeResourceDefinition(snapshotRaw.getSnaphotDfn().getRscDfn());

            SnapshotDefinition snapshotDfn = mergeSnapshotDefinition(snapshotRaw.getSnaphotDfn(), rscDfn);

            mergeSnapshot(snapshotRaw, snapshotDfn);

            rscDfnMap.put(rscDfn.getName(), rscDfn);

            transMgrProvider.get().commit();

            errorReporter.logInfo(
                "Snapshot '%s' of resource '%s' registered.",
                snapshotDfn.getName().displayValue,
                rscDfn.getName().displayValue
            );
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
        SnapshotName snapName = new SnapshotName(snapshotDfnApi.getSnapshotName());

        SnapshotDefinition snapDfn = rscDfn.getSnapshotDfn(apiCtx, snapName);
        if (snapDfn == null)
        {
            snapDfn = snapDfnFactory.getInstanceSatellite(
                apiCtx,
                snapshotDfnApi.getUuid(),
                rscDfn,
                snapName,
                new SnapshotDefinition.Flags[]{}
            );

            rscDfn.addSnapshotDfn(apiCtx, snapDfn);
        }
        checkUuid(snapDfn, snapshotDfnApi);

        PropsUtils.resetProps(snapshotDfnApi.getSnapDfnProps(), snapDfn.getSnapDfnProps(apiCtx));
        PropsUtils.resetProps(snapshotDfnApi.getRscDfnProps(), snapDfn.getRscDfnPropsForChange(apiCtx));

        snapDfn.getFlags().resetFlagsTo(apiCtx, SnapshotDefinition.Flags.restoreFlags(snapshotDfnApi.getFlags()));
        errorReporter.logTrace(
            "resetting flags of local snapdfn (%s) to %s",
            snapDfn,
            FlagsHelper.toStringList(SnapshotDefinition.Flags.class, snapshotDfnApi.getFlags())
        );

        // Merge satellite volume definitions
        Set<VolumeNumber> oldVlmNrs = snapDfn.getAllSnapshotVolumeDefinitions(apiCtx)
            .stream()
            .map(SnapshotVolumeDefinition::getVolumeNumber)
            .collect(Collectors.toCollection(HashSet::new));

        for (SnapshotVolumeDefinitionApi snapVlmDfnApi : snapshotDfnApi.getSnapshotVlmDfnList())
        {
            VolumeNumber vlmNr = new VolumeNumber(snapVlmDfnApi.getVolumeNr());
            oldVlmNrs.remove(vlmNr);

            SnapshotVolumeDefinition.Flags[] snapVlmDfnFlags = SnapshotVolumeDefinition.Flags.restoreFlags(
                snapVlmDfnApi.getFlags()
            );

            SnapshotVolumeDefinition snapVlmDfn = snapDfn.getSnapshotVolumeDefinition(apiCtx, vlmNr);
            if (snapVlmDfn == null)
            {
                snapVlmDfn = snapVlmDfnFactory.getInstanceSatellite(
                    apiCtx,
                    snapVlmDfnApi.getUuid(),
                    snapDfn,
                    rscDfn.getVolumeDfn(apiCtx, vlmNr),
                    vlmNr,
                    snapVlmDfnApi.getSize(),
                    snapVlmDfnFlags
                );
            }

            PropsUtils.resetProps(snapVlmDfnApi.getSnapVlmDfnPropsMap(), snapVlmDfn.getSnapVlmDfnProps(apiCtx));
            PropsUtils.resetProps(snapVlmDfnApi.getVlmDfnPropsMap(), snapVlmDfn.getVlmDfnPropsForChange(apiCtx));

            snapVlmDfn.getFlags().resetFlagsTo(apiCtx, snapVlmDfnFlags);
        }

        for (VolumeNumber oldVolumeNumber : oldVlmNrs)
        {
            snapDfn.removeSnapshotVolumeDefinition(apiCtx, oldVolumeNumber);
        }

        return snapDfn;
    }

    private void mergeSnapshot(SnapshotPojo snapshotRaw, SnapshotDefinition snapshotDfn)
        throws DivergentUuidsException, AccessDeniedException, ValueOutOfRangeException, InvalidNameException,
            DatabaseException
    {
        Node localNode = controllerPeerConnector.getLocalNode();
        Snapshot snap = snapshotDfn.getSnapshot(apiCtx, localNode.getName());
        Snapshot.Flags[] snapshotFlags = Snapshot.Flags.restoreFlags(snapshotRaw.getFlags());
        if (snap == null)
        {
            snap = snapshotFactory.getInstanceSatellite(
                apiCtx,
                snapshotRaw.getSnapshotUuid(),
                localNode,
                snapshotDfn,
                snapshotFlags
            );
        }
        checkUuid(snap, snapshotRaw);
        snap.getFlags().resetFlagsTo(apiCtx, snapshotFlags);
        errorReporter.logTrace(
            "resetting flags of local snapshot (%s) to %s, suspendIO: %b, takeSnapshot: %b",
            snap,
            FlagsHelper.toStringList(Snapshot.Flags.class, snapshotRaw.getFlags()),
            snapshotRaw.getSuspendResource(),
            snapshotRaw.getTakeSnapshot()
        );
        snap.setSuspendResource(apiCtx, snapshotRaw.getSuspendResource());
        snap.setTakeSnapshot(apiCtx, snapshotRaw.getTakeSnapshot());

        PropsUtils.resetProps(snapshotRaw.getSnapPropsMap(), snap.getSnapProps(apiCtx));
        PropsUtils.resetProps(snapshotRaw.getRscPropsMap(), snap.getRscPropsForChange(apiCtx));

        for (SnapshotVolumeApi snapshotVlmApi : snapshotRaw.getSnapshotVlmList())
        {
            mergeSnapVlm(snapshotVlmApi, snap);
        }

        layerSnapDataMerger.mergeLayerData(snap, snapshotRaw.getLayerData(), false);
    }

    private void mergeSnapVlm(SnapshotVolumeApi snapVlmApi, Snapshot snap)
        throws ValueOutOfRangeException, DivergentUuidsException, InvalidNameException, AccessDeniedException
    {
        VolumeNumber vlmNr = new VolumeNumber(snapVlmApi.getSnapshotVlmNr());
        SnapshotVolume snapVlm = snap.getVolume(vlmNr);
        if (snapVlm == null)
        {
            snapVlm = snapVlmFactory.getInstanceSatellite(
                apiCtx,
                snapVlmApi.getSnapshotVlmUuid(),
                snap,
                snap.getSnapshotDefinition().getSnapshotVolumeDefinition(apiCtx, vlmNr)
            );

            snap.putVolume(apiCtx, snapVlm);
        }

        PropsUtils.resetProps(snapVlmApi.getSnapVlmPropsMap(), snapVlm.getSnapVlmProps(apiCtx));
        PropsUtils.resetProps(snapVlmApi.getVlmPropsMap(), snapVlm.getVlmPropsForChange(apiCtx));

        checkUuid(snapVlm, snapVlmApi);
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
                    deleteSnapshotsAndCleanup(
                        rscDfnMap,
                        rscGrpMap,
                        snapDfn,
                        apiCtx,
                        errorReporter,
                        snapShipService,
                        backupShippingMgr
                    );
                }
                transMgrProvider.get().commit();
            }

            deviceManager.snapshotUpdateApplied(
                Collections.singleton(new SnapshotDefinition.Key(rscName, snapshotName))
            );
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
        }
    }

    public static void deleteSnapshotsAndCleanup(
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CoreModule.ResourceGroupMap rscGrpMapRef,
        SnapshotDefinition snapDfn,
        AccessContext accCtx,
        ErrorReporter errorReporterRef,
        SnapshotShippingService snapshipServiceRef,
        BackupShippingMgr backupShippingMgrRef
    )
        throws AccessDeniedException, DatabaseException
    {
        ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
        SnapshotName snapName = snapDfn.getName();
        ArrayList<Snapshot> copyOfSnapshots = new ArrayList<>(snapDfn.getAllSnapshots(accCtx));
        for (Snapshot snap : copyOfSnapshots)
        {
            snapshipServiceRef.snapshotDeleted(snap);
            backupShippingMgrRef.snapshotDeleted(snap);
            snap.delete(accCtx);
        }
        snapDfn.delete(accCtx);

        errorReporterRef.logInfo("Snapshot '%s' deleted.", snapName);

        if (rscDfn.getResourceCount() == 0 && rscDfn.getSnapshotDfns(accCtx).isEmpty())
        {
            ResourceName rscName = rscDfn.getName();
            @Nullable ResourceDefinition removedRscDfn = rscDfnMapRef.remove(rscName);
            if (removedRscDfn != null)
            {
                ResourceGroup rscGrp = removedRscDfn.getResourceGroup();
                removedRscDfn.delete(accCtx);
                errorReporterRef.logInfo(
                    "Resource definition '%s' deleted (triggered by deletion of snapshot '%s')",
                    rscName,
                    snapName
                );
                if (!rscGrp.hasResourceDefinitions(accCtx))
                {
                    ResourceGroupName rscGrpName = rscGrp.getName();
                    errorReporterRef.logInfo(
                        "Resource group '%s' deleted (triggered by deletion of resource definition '%s')",
                        rscGrpName,
                        rscName
                    );
                    rscGrpMapRef.remove(rscGrpName);
                    rscGrp.delete(accCtx);
                }
            }
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
