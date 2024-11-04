package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.ExternalFilePojo;
import com.linbit.linstor.core.ControllerPeerConnectorImpl;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.core.objects.ExternalFileSatelliteFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Singleton
class StltExternalFilesApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ExternalFileSatelliteFactory extFileFactory;
    private final ControllerPeerConnectorImpl ctrlPeerConnector;
    private final DeviceManager deviceManager;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CoreModule.ExternalFileMap extFileMap;

    @Inject
    StltExternalFilesApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        CoreModule.ExternalFileMap extFileMapRef,
        ExternalFileSatelliteFactory extFileFactoryRef,
        ControllerPeerConnectorImpl ctrlPeerConnectorRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        extFileFactory = extFileFactoryRef;
        ctrlPeerConnector = ctrlPeerConnectorRef;

        rscDfnMap = rscDfnMapRef;
        extFileMap = extFileMapRef;
        transMgrProvider = transMgrProviderRef;
    }

    public void applyChanges(ExternalFilePojo extFilePojo)
    {
        try
        {
            @Nullable byte[] newContent = extFilePojo.getContent();
            ExternalFile localExtFile = extFileFactory.getInstanceSatellite(
                apiCtx,
                extFilePojo.getUuid(),
                LinstorParsingUtils.asExtFileName(extFilePojo.getFileName()),
                extFilePojo.getFlags(),
                newContent
            );

            localExtFile.getFlags().resetFlagsTo(apiCtx, ExternalFile.Flags.restoreFlags(extFilePojo.getFlags()));
            if (newContent != null && newContent.length > 0)
            {
                localExtFile.setContent(apiCtx, newContent);
                localExtFile.setAlreadyWritten(false);
            }

            if (!Arrays.equals(localExtFile.getContentCheckSum(apiCtx), extFilePojo.getContentChecksum()))
            {
                deviceManager.getUpdateTracker().updateExternalFile(localExtFile.getUuid(), localExtFile.getName());
            }

            // TODO maybe only add rscNames that also have requested this extFile to be placed.
            Set<ResourceName> rscNameSet = new HashSet<>(rscDfnMap.keySet());
            deviceManager.externalFileUpdateApplied(
                localExtFile.getName(),
                ctrlPeerConnector.getLocalNodeName(),
                rscNameSet
            );

            transMgrProvider.get().commit();
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
        }
    }

    public void applyDeletedExtFile(ExternalFilePojo externalFilePojoRef)
    {
        try
        {
            ExternalFileName extFileName = new ExternalFileName(externalFilePojoRef.getFileName());
            ExternalFile extFile = extFileMap.get(extFileName);
            if (extFile != null)
            {
                extFile.delete(apiCtx);
                extFileMap.remove(extFileName);
                transMgrProvider.get().commit();
            }
        }
        catch (InvalidNameException | AccessDeniedException | DatabaseException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
        }
    }
}
