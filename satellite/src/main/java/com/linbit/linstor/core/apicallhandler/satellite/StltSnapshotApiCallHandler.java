package com.linbit.linstor.core.apicallhandler.satellite;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.DivergentUuidsException;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.NodeData;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinitionDataSatelliteFactory;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDataSatelliteFactory;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinitionDataSatelliteFactory;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDataSatelliteFactory;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.core.objects.SnapshotDefinition.SnapshotDfnFlags;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition.SnapshotVlmDfnFlags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

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
    private final ResourceDefinitionDataSatelliteFactory resourceDefinitionDataFactory;
    private final SnapshotDefinitionDataSatelliteFactory snapshotDefinitionDataFactory;
    private final SnapshotVolumeDefinitionSatelliteFactory snapshotVolumeDefinitionFactory;
    private final SnapshotDataSatelliteFactory snapshotDataFactory;
    private final SnapshotVolumeDataSatelliteFactory snapshotVolumeDataFactory;
    private final StltRscGrpApiCallHelper rscGrpApiCallHelper;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    StltSnapshotApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        CoreModule.ResourceGroupMap rscGrpMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        ResourceDefinitionDataSatelliteFactory resourceDefinitionDataFactoryRef,
        SnapshotDefinitionDataSatelliteFactory snapshotDefinitionDataFactoryRef,
        SnapshotVolumeDefinitionSatelliteFactory snapshotVolumeDefinitionFactoryRef,
        SnapshotDataSatelliteFactory snapshotDataFactoryRef,
        SnapshotVolumeDataSatelliteFactory snapshotVolumeDataFactoryRef,
        StltRscGrpApiCallHelper stltGrpApiCallHelperRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        rscGrpMap = rscGrpMapRef;
        rscDfnMap = rscDfnMapRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        resourceDefinitionDataFactory = resourceDefinitionDataFactoryRef;
        snapshotDefinitionDataFactory = snapshotDefinitionDataFactoryRef;
        snapshotVolumeDefinitionFactory = snapshotVolumeDefinitionFactoryRef;
        snapshotDataFactory = snapshotDataFactoryRef;
        snapshotVolumeDataFactory = snapshotVolumeDataFactoryRef;
        rscGrpApiCallHelper = stltGrpApiCallHelperRef;
        transMgrProvider = transMgrProviderRef;
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
                new SnapshotDefinition.Key(rscDfn.getName(), snapshotDfn.getName())));
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
        }
    }

    private ResourceDefinition mergeResourceDefinition(ResourceDefinition.RscDfnApi rscDfnApi)
        throws InvalidNameException, DivergentUuidsException, AccessDeniedException, DatabaseException
    {
        ResourceName rscName = new ResourceName(rscDfnApi.getResourceName());

        RscDfnFlags[] rscDfnFlags = RscDfnFlags.restoreFlags(rscDfnApi.getFlags());

        ResourceGroup rscGrp = rscGrpApiCallHelper.mergeResourceGroup(rscDfnApi.getResourceGroup());

        ResourceDefinition rscDfn = rscDfnMap.get(rscName);
        if (rscDfn == null)
        {
            rscDfn = resourceDefinitionDataFactory.getInstanceSatellite(
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
        SnapshotDefinition.SnapshotDfnApi snapshotDfnApi,
        ResourceDefinition rscDfn
    )
        throws AccessDeniedException, DivergentUuidsException, InvalidNameException, ValueOutOfRangeException,
            DatabaseException
    {
        SnapshotName snapshotName = new SnapshotName(snapshotDfnApi.getSnapshotName());

        SnapshotDefinition snapshotDfn = rscDfn.getSnapshotDfn(apiCtx, snapshotName);
        if (snapshotDfn == null)
        {
            snapshotDfn = snapshotDefinitionDataFactory.getInstanceSatellite(
                apiCtx,
                snapshotDfnApi.getUuid(),
                rscDfn,
                snapshotName,
                new SnapshotDfnFlags[]{}
            );

            rscDfn.addSnapshotDfn(apiCtx, snapshotDfn);
        }
        checkUuid(snapshotDfn, snapshotDfnApi);

        Props snapshotDfnProps = snapshotDfn.getProps(apiCtx);
        snapshotDfnProps.map().putAll(snapshotDfnApi.getProps());
        snapshotDfnProps.keySet().retainAll(snapshotDfnApi.getProps().keySet());

        snapshotDfn.getFlags().resetFlagsTo(apiCtx, SnapshotDfnFlags.restoreFlags(snapshotDfnApi.getFlags()));

        // Merge satellite volume definitions
        Set<VolumeNumber> oldVolumeNumbers = snapshotDfn.getAllSnapshotVolumeDefinitions(apiCtx).stream()
            .map(SnapshotVolumeDefinition::getVolumeNumber)
            .collect(Collectors.toCollection(HashSet::new));

        for (SnapshotVolumeDefinition.SnapshotVlmDfnApi snapshotVlmDfnApi : snapshotDfnApi.getSnapshotVlmDfnList())
        {
            VolumeNumber volumeNumber = new VolumeNumber(snapshotVlmDfnApi.getVolumeNr());
            oldVolumeNumbers.remove(volumeNumber);

            SnapshotVlmDfnFlags[] snapshotVlmDfnFlags = SnapshotVlmDfnFlags.restoreFlags(snapshotVlmDfnApi.getFlags());

            SnapshotVolumeDefinition snapshotVolumeDefinition =
                snapshotDfn.getSnapshotVolumeDefinition(apiCtx, volumeNumber);
            if (snapshotVolumeDefinition == null)
            {
                snapshotVolumeDefinition = snapshotVolumeDefinitionFactory.getInstanceSatellite(
                    apiCtx,
                    snapshotVlmDfnApi.getUuid(),
                    snapshotDfn,
                    volumeNumber,
                    snapshotVlmDfnApi.getSize(),
                    snapshotVlmDfnFlags
                );
            }
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
        NodeData localNode = controllerPeerConnector.getLocalNode();
        Snapshot snapshot = snapshotDfn.getSnapshot(apiCtx, localNode.getName());
        Snapshot.SnapshotFlags[] snapshotFlags = Snapshot.SnapshotFlags.restoreFlags(snapshotRaw.getFlags());
        if (snapshot == null)
        {
            snapshot = snapshotDataFactory.getInstanceSatellite(
                apiCtx,
                snapshotRaw.getSnapshotUuid(),
                localNode,
                snapshotDfn,
                snapshotFlags
            );
        }
        checkUuid(snapshot, snapshotRaw);
        snapshot.getFlags().resetFlagsTo(apiCtx, snapshotFlags);

        snapshot.setSuspendResource(apiCtx, snapshotRaw.getSuspendResource());
        snapshot.setTakeSnapshot(apiCtx, snapshotRaw.getTakeSnapshot());

        for (SnapshotVolume.SnapshotVlmApi snapshotVlmApi : snapshotRaw.getSnapshotVlmList())
        {
            mergeSnapshotVolume(snapshotVlmApi, snapshot);
        }
    }

    private void mergeSnapshotVolume(SnapshotVolume.SnapshotVlmApi snapshotVlmApi, Snapshot snapshot)
        throws ValueOutOfRangeException, DivergentUuidsException, InvalidNameException, AccessDeniedException
    {
        VolumeNumber volumeNumber = new VolumeNumber(snapshotVlmApi.getSnapshotVlmNr());
        SnapshotVolume snapshotVolume = snapshot.getSnapshotVolume(apiCtx, volumeNumber);
        if (snapshotVolume == null)
        {
            StorPool storPool = snapshot.getNode().getStorPool(
                apiCtx,
                new StorPoolName(snapshotVlmApi.getStorPoolName())
            );
            checkUuid(storPool, snapshotVlmApi);

            snapshotVolume = snapshotVolumeDataFactory.getInstanceSatellite(
                apiCtx,
                snapshotVlmApi.getSnapshotVlmUuid(),
                snapshot,
                snapshot.getSnapshotDefinition().getSnapshotVolumeDefinition(apiCtx, volumeNumber),
                storPool
            );

            snapshot.addSnapshotVolume(apiCtx, snapshotVolume);
        }
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
                rscDfn.removeSnapshotDfn(apiCtx, snapshotName);

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

    private void checkUuid(ResourceDefinition rscDfn, ResourceDefinition.RscDfnApi rscDfnApi)
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

    private void checkUuid(SnapshotDefinition snapshotDfn, SnapshotDefinition.SnapshotDfnApi snapshotDfnApi)
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

    private void checkUuid(SnapshotVolume snapshotVolume, SnapshotVolume.SnapshotVlmApi snapshotVlmApi)
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

    private void checkUuid(StorPool storPool, SnapshotVolume.SnapshotVlmApi snapshotVlmApi)
        throws DivergentUuidsException
    {
        checkUuid(
            storPool.getUuid(),
            snapshotVlmApi.getStorPoolUuid(),
            "StorPool",
            storPool.getName().displayValue,
            snapshotVlmApi.getStorPoolName()
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
