package com.linbit.linstor.backupshipping;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.BackupToS3;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.StltConnTracker;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.locks.LockGuardFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.text.ParseException;
import java.util.function.BiConsumer;

import com.amazonaws.SdkClientException;

@Singleton
public class BackupShippingS3Service extends AbsBackupShippingService
{
    public static final String SERVICE_INFO = "BackupShippingS3Service";

    protected static final String CMD_FORMAT_SENDING =
        "set -o pipefail; " +
            "%s | " +  // thin_send prev_LV_snapshot cur_LV_snapshot
            // "pv -s 100m -bnr -i 0.1 | " +
            "zstd;";

    protected static final String CMD_FORMAT_RECEIVING =
        "set -o pipefail; " +
        "zstd -d | " +
        // "pv -s 100m -bnr -i 0.1 | " +
        "%s";

    private final BackupToS3 backupHandler;

    @Inject
    public BackupShippingS3Service(
        BackupToS3 backupHandlerRef,
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer interComSerializerRef,
        @SystemContext AccessContext accCtxRef,
        StltSecurityObjects stltSecObjRef,
        StltConfigAccessor stltConfigAccessorRef,
        StltConnTracker stltConnTracker,
        RemoteMap remoteMapRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        super(
            errorReporterRef,
            SERVICE_INFO,
            RemoteType.S3,
            extCmdFactoryRef,
            controllerPeerConnectorRef,
            interComSerializerRef,
            accCtxRef,
            stltSecObjRef,
            stltConfigAccessorRef,
            stltConnTracker,
            remoteMapRef,
            lockGuardFactoryRef
        );

        backupHandler = backupHandlerRef;
    }

    @Override
    protected String getCommandReceiving(String cmdRef, AbsRemote ignoredRemote, AbsStorageVlmData<Snapshot> ignored)
    {
        return String.format(CMD_FORMAT_RECEIVING, cmdRef);
    }

    @Override
    protected String getCommandSending(String cmdRef, AbsRemote ignoredRemote, AbsStorageVlmData<Snapshot> ignored)
    {
        return String.format(CMD_FORMAT_SENDING, cmdRef);
    }

    @Override
    protected String getBackupNameForRestore(AbsStorageVlmData<Snapshot> snapVlmDataRef)
        throws InvalidKeyException, AccessDeniedException
    {
        Snapshot snap = snapVlmDataRef.getVolume().getAbsResource();
        String backupId = snap.getSnapshotDefinition()
            .getSnapDfnProps(accCtx)
            .getProp(
                InternalApiConsts.KEY_BACKUP_TO_RESTORE,
                BackupShippingUtils.BACKUP_TARGET_PROPS_NAMESPC
            );

        try
        {
            S3MetafileNameInfo info = new S3MetafileNameInfo(backupId);

            return new S3VolumeNameInfo(
                info.rscName,
                snapVlmDataRef.getRscLayerObject().getResourceNameSuffix(),
                snapVlmDataRef.getVlmNr().value,
                info.backupTime,
                info.s3Suffix,
                info.snapName
            ).toString();
        }
        catch (ParseException exc)
        {
            throw new ImplementationError(
                "The simplified backup-name " + backupId + " does not conform to the expected format."
            );
        }
    }

    @Override
    protected BackupShippingDaemon createDaemon(
        AbsStorageVlmData<Snapshot> snapVlmDataRef,
        String[] fullCommand,
        String backupNameRef,
        AbsRemote remote,
        boolean restore,
        Integer ignored,
        BiConsumer<Boolean, Integer> postAction
    )
    {
        return new BackupShippingS3Daemon(
            errorReporter,
            threadGroup,
            "shipping_" + backupNameRef,
            fullCommand,
            backupNameRef,
            (S3Remote) remote,
            backupHandler,
            restore,
            snapVlmDataRef.getAllocatedSize() == -1 && snapVlmDataRef.getSnapshotAllocatedSize() != null ?
                snapVlmDataRef.getSnapshotAllocatedSize() :
                snapVlmDataRef.getAllocatedSize(),
            postAction,
            accCtx,
            stltSecObj.getCryptKey()
        );
    }

    @Override
    protected boolean preCtrlNotifyBackupShipped(
        boolean success,
        boolean restoring,
        Snapshot snap,
        ShippingInfo shippingInfo
    )
    {
        boolean successRet = success;
        if (success && !restoring)
        {
            try
            {
                S3Remote s3Remote = (S3Remote) shippingInfo.s3orStltRemote;

                backupHandler.putObject(
                    shippingInfo.s3MetaKey,
                    fillPojo(snap, shippingInfo.basedOnS3MetaKey, shippingInfo),
                    s3Remote,
                    accCtx,
                    stltSecObj.getCryptKey()
                );
            }
            catch (InvalidKeyException | AccessDeniedException | IOException | ParseException exc)
            {
                errorReporter.reportError(new ImplementationError(exc));
                successRet = false;
            }
            catch (SdkClientException exc)
            {
                errorReporter.reportError(exc);
                successRet = false;
            }
        }
        return successRet;
    }

    @Override
    protected void postAllBackupPartsRegistered(Snapshot snapRef, ShippingInfo infoRef)
    {
        // ignored
    }
}
