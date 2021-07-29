package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.api.pojo.StltRemotePojo;
import com.linbit.linstor.core.ControllerPeerConnectorImpl;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Remote;
import com.linbit.linstor.core.objects.S3Remote;
import com.linbit.linstor.core.objects.S3RemoteSatelliteFactory;
import com.linbit.linstor.core.objects.StltRemote;
import com.linbit.linstor.core.objects.StltRemoteSatelliteFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class StltRemoteApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
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
        StltRemoteSatelliteFactory stltRemoteFactoryRef,
        ControllerPeerConnectorImpl ctrlPeerConnectorRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        s3remoteFactory = s3remoteFactoryRef;
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

            deviceManager.remoteUpdateApplied(
                localS3Remote.getName(),
                ctrlPeerConnector.getLocalNodeName()
            );

            transMgrProvider.get().commit();
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
            Remote remote = remoteMap.get(remoteName);
            if (remote != null)
            {
                remote.delete(apiCtx);
                remoteMap.remove(remoteName);
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
                stltRemotePojo.getFlags(),
                stltRemotePojo.getIp(),
                stltRemotePojo.getPort(),
                stltRemotePojo.useZstd()
            );

            localStltRemote.applyApiData(apiCtx, stltRemotePojo);

            deviceManager.remoteUpdateApplied(
                localStltRemote.getName(),
                ctrlPeerConnector.getLocalNodeName()
            );

            transMgrProvider.get().commit();
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
            Remote remote = remoteMap.get(remoteName);
            if (remote != null)
            {
                remote.delete(apiCtx);
                remoteMap.remove(remoteName);
            }
        }
        catch (InvalidNameException | AccessDeniedException | DatabaseException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
        }
    }
}
