package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.ResourceDefinitionDataSatelliteFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDataSatelliteFactory;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotDefinitionDataSatelliteFactory;
import com.linbit.linstor.SnapshotId;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

@Singleton
class StltSnapshotApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final ControllerPeerConnector controllerPeerConnector;
    private final ResourceDefinitionDataSatelliteFactory resourceDefinitionDataFactory;
    private final SnapshotDefinitionDataSatelliteFactory snapshotDefinitionDataFactory;
    private final SnapshotDataSatelliteFactory snapshotDataFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    StltSnapshotApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        ResourceDefinitionDataSatelliteFactory resourceDefinitionDataFactoryRef,
        SnapshotDefinitionDataSatelliteFactory snapshotDefinitionDataFactoryRef,
        SnapshotDataSatelliteFactory snapshotDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        rscDfnMap = rscDfnMapRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        resourceDefinitionDataFactory = resourceDefinitionDataFactoryRef;
        snapshotDefinitionDataFactory = snapshotDefinitionDataFactoryRef;
        snapshotDataFactory = snapshotDataFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public void applyChanges(SnapshotPojo snapshotRaw)
    {
        try
        {
            ResourceDefinition rscDfnToRegister = null;

            SnapshotName snapshotName = new SnapshotName(snapshotRaw.getSnapshotName());

            ResourceDefinition.RscDfnApi rscDfnApi = snapshotRaw.getRscDfn();
            ResourceName rscName = new ResourceName(rscDfnApi.getResourceName());

            String rscDfnSecret = rscDfnApi.getSecret();
            TransportType rscDfnTransportType = TransportType.byValue(rscDfnApi.getTransportType());
            TcpPortNumber port = new TcpPortNumber(rscDfnApi.getPort());
            RscDfnFlags[] rscDfnFlags = RscDfnFlags.restoreFlags(rscDfnApi.getFlags());

            ResourceDefinition rscDfn = rscDfnMap.get(rscName);
            if (rscDfn == null)
            {
                rscDfn = resourceDefinitionDataFactory.getInstanceSatellite(
                    apiCtx,
                    rscDfnApi.getUuid(),
                    rscName,
                    port,
                    rscDfnFlags,
                    rscDfnSecret,
                    rscDfnTransportType
                );

                checkUuid(rscDfn, snapshotRaw);
                rscDfnToRegister = rscDfn;
            }
            rscDfn.setPort(apiCtx, port);
            Map<String, String> rscDfnProps = rscDfn.getProps(apiCtx).map();
            rscDfnProps.clear();
            rscDfnProps.putAll(rscDfnApi.getProps());
            rscDfn.getFlags().resetFlagsTo(apiCtx, rscDfnFlags);

            SnapshotDefinition snapshotDfn = rscDfn.getSnapshotDfn(apiCtx, snapshotName);
            if (snapshotDfn == null)
            {
                snapshotDfn = snapshotDefinitionDataFactory.getInstanceSatellite(
                    apiCtx,
                    snapshotRaw.getSnapshotDfnUuid(),
                    rscDfn,
                    snapshotName,
                    new SnapshotDefinition.SnapshotDfnFlags[]{}
                );

                rscDfn.addSnapshotDfn(apiCtx, snapshotDfn);
            }
            checkUuid(snapshotDfn, snapshotRaw);

            NodeData localNode = controllerPeerConnector.getLocalNode();
            Snapshot snapshot = snapshotDfn.getSnapshot(localNode.getName());
            if (snapshot == null)
            {
                snapshot = snapshotDataFactory.getInstanceSatellite(
                    apiCtx,
                    snapshotRaw.getSnapshotUuid(),
                    localNode,
                    snapshotDfn
                );
            }
            checkUuid(snapshot, snapshotRaw);

            snapshot.setSuspendResource(snapshotRaw.getSuspendResource());
            snapshot.setTakeSnapshot(snapshotRaw.getTakeSnapshot());

            transMgrProvider.get().commit();

            errorReporter.logInfo(
                "Snapshot '%s' registered.",
                snapshotName.displayValue
            );

            if (rscDfnToRegister != null)
            {
                rscDfnMap.put(rscName, rscDfnToRegister);
            }

            deviceManager.getUpdateTracker().checkResource(rscName);
            deviceManager.snapshotUpdateApplied(Collections.singleton(new SnapshotId(rscName, snapshotName)));
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
        }
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
                        removedRscDfn.delete(apiCtx);
                    }
                }

                transMgrProvider.get().commit();

                errorReporter.logInfo(
                    "Snapshot '%s' ended.",
                    snapshotName
                );

                deviceManager.snapshotUpdateApplied(Collections.singleton(new SnapshotId(rscName, snapshotName)));
            }
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
        }
    }

    private void checkUuid(ResourceDefinition rscDfn, SnapshotPojo snapshotRaw)
        throws DivergentUuidsException
    {
        checkUuid(
            rscDfn.getUuid(),
            snapshotRaw.getRscDfn().getUuid(),
            "ResourceDefinition",
            rscDfn.getName().displayValue,
            snapshotRaw.getRscDfn().getResourceName()
        );
    }

    private void checkUuid(SnapshotDefinition snapshotDfn, SnapshotPojo snapshotRaw)
        throws DivergentUuidsException
    {
        checkUuid(
            snapshotDfn.getUuid(),
            snapshotRaw.getSnapshotDfnUuid(),
            "SnapshotDefinition",
            snapshotDfn.getName().displayValue,
            snapshotRaw.getSnapshotName()
        );
    }

    private void checkUuid(Snapshot snapshot, SnapshotPojo snapshotRaw)
        throws DivergentUuidsException, AccessDeniedException
    {
        checkUuid(
            snapshot.getUuid(),
            snapshotRaw.getSnapshotUuid(),
            "Snapshot",
            snapshot.getSnapshotDefinition().getName().displayValue,
            snapshotRaw.getSnapshotName()
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
