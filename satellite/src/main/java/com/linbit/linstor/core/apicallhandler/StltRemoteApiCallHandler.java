package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.pojo.EbsRemotePojo;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.api.pojo.StltRemotePojo;
import com.linbit.linstor.core.ControllerPeerConnectorImpl;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.EbsRemoteSatelliteFactory;
import com.linbit.linstor.core.objects.S3RemoteSatelliteFactory;
import com.linbit.linstor.core.objects.StltRemoteSatelliteFactory;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.net.URL;

@Singleton
public class StltRemoteApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final EbsRemoteSatelliteFactory ebsRemoteFactory;
    private final S3RemoteSatelliteFactory s3remoteFactory;
    private final StltRemoteSatelliteFactory stltRemoteFactory;
    private final ControllerPeerConnectorImpl ctrlPeerConnector;
    private final DeviceManager deviceManager;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CoreModule.RemoteMap remoteMap;

    @Inject
    StltRemoteApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        CoreModule.RemoteMap remoteMapRef,
        S3RemoteSatelliteFactory s3remoteFactoryRef,
        EbsRemoteSatelliteFactory ebsRemoteFactoryRef,
        StltRemoteSatelliteFactory stltRemoteFactoryRef,
        ControllerPeerConnectorImpl ctrlPeerConnectorRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        s3remoteFactory = s3remoteFactoryRef;
        ebsRemoteFactory = ebsRemoteFactoryRef;
        stltRemoteFactory = stltRemoteFactoryRef;
        ctrlPeerConnector = ctrlPeerConnectorRef;

        remoteMap = remoteMapRef;
        transMgrProvider = transMgrProviderRef;
    }

    public void applyChangesS3(S3RemotePojo s3remotePojo)
    {
        try
        {
            S3Remote localS3Remote = s3remoteFactory.getInstanceSatellite(
                apiCtx,
                s3remotePojo.getUuid(),
                LinstorParsingUtils.asRemoteName(s3remotePojo.getRemoteName()),
                s3remotePojo.getFlags(),
                s3remotePojo.getEndpoint(),
                s3remotePojo.getBucket(),
                s3remotePojo.getRegion(),
                s3remotePojo.getAccessKey(),
                s3remotePojo.getSecretKey()
            );

            localS3Remote.applyApiData(apiCtx, s3remotePojo);

            RemoteName remoteName = localS3Remote.getName();
            if (localS3Remote.getFlags().isSet(apiCtx, StltRemote.Flags.DELETE))
            {
                errorReporter.logTrace("S3Remote marked for deletion. Deleting. %s", remoteName);
                remoteMap.remove(remoteName);
                localS3Remote.delete(apiCtx);
            }
            transMgrProvider.get().commit();

            deviceManager.remoteUpdateApplied(
                remoteName,
                ctrlPeerConnector.getLocalNodeName()
            );
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
        }
    }

    public void applyChangesEbs(EbsRemotePojo ebsRemotePojo)
    {
        try
        {
            EbsRemote localEbsRemote = ebsRemoteFactory.getInstanceSatellite(
                apiCtx,
                ebsRemotePojo.getUuid(),
                new RemoteName(ebsRemotePojo.getRemoteName(), true),
                ebsRemotePojo.getFlags(),
                new URL(ebsRemotePojo.getUrl()),
                ebsRemotePojo.getRegion(),
                ebsRemotePojo.getAvailabilityZone(),
                ebsRemotePojo.getAccessKey(),
                ebsRemotePojo.getSecretKey()
            );

            localEbsRemote.applyApiData(apiCtx, ebsRemotePojo);

            RemoteName remoteName = localEbsRemote.getName();
            if (localEbsRemote.getFlags().isSet(apiCtx, StltRemote.Flags.DELETE))
            {
                errorReporter.logTrace("EbsRemote marked for deletion. Deleting. %s", remoteName);
                remoteMap.remove(remoteName);
                localEbsRemote.delete(apiCtx);
            }
            transMgrProvider.get().commit();

            deviceManager.remoteUpdateApplied(
                remoteName,
                ctrlPeerConnector.getLocalNodeName()
            );
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
        }
    }

    public void applyDeletedRemote(String remoteNameStrRef)
    {
        try
        {
            RemoteName remoteName = new RemoteName(remoteNameStrRef, true);
            AbsRemote remote = remoteMap.get(remoteName);
            if (remote != null)
            {
                errorReporter.logTrace("Cleaning up deleted Remote: %s", remoteNameStrRef);
                remote.delete(apiCtx);
                remoteMap.remove(remoteName);
                transMgrProvider.get().commit();
            }
        }
        catch (InvalidNameException | AccessDeniedException | DatabaseException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
        }
    }

    public void applyChangesStlt(StltRemotePojo stltRemotePojo)
    {
        try
        {
            StltRemote localStltRemote = stltRemoteFactory.getInstanceSatellite(
                apiCtx,
                stltRemotePojo.getUuid(),
                new RemoteName(stltRemotePojo.getRemoteName(), true),
                new RemoteName(stltRemotePojo.getLinRemoteName()),
                null, // stlt does not need the node
                stltRemotePojo.getFlags(),
                stltRemotePojo.getIp(),
                stltRemotePojo.getPorts(),
                stltRemotePojo.useZstd()
            );

            localStltRemote.applyApiData(apiCtx, stltRemotePojo);

            RemoteName remoteName = localStltRemote.getName();
            if (localStltRemote.getFlags().isSet(apiCtx, StltRemote.Flags.DELETE))
            {
                errorReporter.logTrace("StltRemote marked for deletion. Deleting. %s", remoteName);
                remoteMap.remove(remoteName);
                localStltRemote.delete(apiCtx);
            }

            transMgrProvider.get().commit();

            deviceManager.remoteUpdateApplied(
                remoteName,
                ctrlPeerConnector.getLocalNodeName()
            );
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
        }
    }

    public void applyDeletedStltRemote(StltRemotePojo stltRemotePojoRef)
    {
        try
        {
            RemoteName remoteName = new RemoteName(stltRemotePojoRef.getRemoteName());
            AbsRemote remote = remoteMap.get(remoteName);
            if (remote != null)
            {
                errorReporter.logTrace("Cleaning up deleted StltRemote: %s", stltRemotePojoRef);
                remote.delete(apiCtx);
                remoteMap.remove(remoteName);
                transMgrProvider.get().commit();
            }
        }
        catch (InvalidNameException | AccessDeniedException | DatabaseException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
        }
    }
}
