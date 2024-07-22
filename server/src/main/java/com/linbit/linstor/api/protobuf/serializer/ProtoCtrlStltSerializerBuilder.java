package com.linbit.linstor.api.protobuf.serializer;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer.CommonSerializerBuilder;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer.CtrlStltSerializerBuilder;
import com.linbit.linstor.api.protobuf.ProtoStorPoolFreeSpaceUtils;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.controller.internal.helpers.AtomicUpdateSatelliteData;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.proto.common.CryptoEntryOuterClass;
import com.linbit.linstor.proto.common.RscLayerDataOuterClass.RscLayerData;
import com.linbit.linstor.proto.common.StltConfigOuterClass;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;
import com.linbit.linstor.proto.javainternal.c2s.IntControllerOuterClass.IntController;
import com.linbit.linstor.proto.javainternal.c2s.IntEbsRemoteOuterClass.IntEbsRemote;
import com.linbit.linstor.proto.javainternal.c2s.IntExternalFileOuterClass.IntExternalFile;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNetIf;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNetIf.Builder;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNode;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNodeConn;
import com.linbit.linstor.proto.javainternal.c2s.IntRscOuterClass.IntOtherRsc;
import com.linbit.linstor.proto.javainternal.c2s.IntRscOuterClass.IntRsc;
import com.linbit.linstor.proto.javainternal.c2s.IntS3RemoteOuterClass.IntS3Remote;
import com.linbit.linstor.proto.javainternal.c2s.IntSnapshotOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.IntSnapshotOuterClass.IntSnapshot;
import com.linbit.linstor.proto.javainternal.c2s.IntStltRemoteOuterClass.IntStltRemote;
import com.linbit.linstor.proto.javainternal.c2s.IntStorPoolOuterClass.IntStorPool;
import com.linbit.linstor.proto.javainternal.c2s.MsgCreateDevicePoolOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.MsgCreateDevicePoolOuterClass.MsgCreateDevicePool;
import com.linbit.linstor.proto.javainternal.c2s.MsgDeleteDevicePoolOuterClass.MsgDeleteDevicePool;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyControllerOuterClass.MsgIntApplyController;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyDeletedNodeOuterClass.MsgIntApplyDeletedNode;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyDeletedRscOuterClass.MsgIntApplyDeletedRsc;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyDeletedStorPoolOuterClass.MsgIntApplyDeletedStorPool;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyExternalFileOuterClass.MsgIntApplyExternalFile;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyFullSyncOuterClass.MsgIntApplyFullSync;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyNodeOuterClass.MsgIntApplyNode;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyRemoteOuterClass.MsgIntApplyRemote;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyRscOuterClass.MsgIntApplyRsc;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplySharedStorPoolLocksOuterClass.MsgIntApplySharedStorPoolLocks;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplySnapshotOuterClass.MsgIntApplySnapshot;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyStorPoolOuterClass.MsgIntApplyStorPool;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntAuthOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntBackupShippingFinishedOuterClass.MsgIntBackupShippingFinished;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntCryptKeyOuterClass.MsgIntCryptKey;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntExternalFileDeletedDataOuterClass.MsgIntExternalFileDeletedData;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntRemoteDeletedOuterClass.MsgIntRemoteDeleted;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntSnapshotEndedDataOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.MsgReqPhysicalDevicesOuterClass.MsgReqPhysicalDevices;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyConfigResponseOuterClass.MsgIntApplyConfigResponse;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyNodeSuccessOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyRscSuccessOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyRscSuccessOuterClass.MsgIntApplyRscSuccess;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyStorPoolSuccessOuterClass.MsgIntApplyStorPoolSuccess;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntBackupRcvReadyOuterClass.MsgIntBackupRcvReady;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntBackupShippedOuterClass.MsgIntBackupShipped;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntBackupShippingIdOuterClass.MsgIntBackupShippingId;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntBackupShippingWrongPortsOuterClass.MsgIntBackupShippingWrongPorts;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntChangedDataOuterClass.ChangedResource;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntChangedDataOuterClass.ChangedSnapshot;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntChangedDataOuterClass.MsgIntChangedData;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntCloneUpdateOuterClass.MsgIntCloneUpdate;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntPrimaryOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntRequestSharedStorPoolLocksOuterClass.MsgIntRequestSharedStorPoolLocks;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntSnapshotShippedOuterClass.MsgIntSnapshotShipped;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntUpdateFreeSpaceOuterClass.MsgIntUpdateFreeSpace;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntUpdateLocalNodeChangeOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgPhysicalDevicesOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgPhysicalDevicesOuterClass.MsgPhysicalDevices;
import com.linbit.linstor.proto.javainternal.s2c.MsgRscFailedOuterClass.MsgRscFailed;
import com.linbit.linstor.proto.javainternal.s2c.MsgSnapshotRollbackResultOuterClass.MsgSnapshotRollbackResult;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.linstor.storage.ProcCryptoEntry;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.RaidLevel;
import com.linbit.linstor.utils.SetUtils;
import com.linbit.utils.Base64;
import com.linbit.utils.Either;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import static java.util.stream.Collectors.toList;

public class ProtoCtrlStltSerializerBuilder extends ProtoCommonSerializerBuilder
    implements CtrlStltSerializer.CtrlStltSerializerBuilder
{
    private final CtrlSerializerHelper ctrlSerializerHelper;
    private final ResourceSerializerHelper rscSerializerHelper;
    private final SnapshotSerializerHelper snapshotSerializerHelper;
    private final NodeSerializerHelper nodeSerializerHelper;
    private final ExternalFileSerializerHelper externalFileSerializerHelper;
    private final RemoteSerializerHelper remoteSerializerHelper;
    private final CtrlSecurityObjects secObjs;

    public ProtoCtrlStltSerializerBuilder(
        ErrorReporter errReporter,
        AccessContext serializerCtx,
        CtrlSecurityObjects secObjsRef,
        ReadOnlyProps ctrlConfRef,
        final String apiCall,
        Long apiCallId,
        boolean isAnswer
    )
    {
        super(errReporter, serializerCtx, apiCall, apiCallId, isAnswer);
        secObjs = secObjsRef;

        ctrlSerializerHelper = new CtrlSerializerHelper(ctrlConfRef);
        rscSerializerHelper = new ResourceSerializerHelper();
        snapshotSerializerHelper = new SnapshotSerializerHelper();
        nodeSerializerHelper = new NodeSerializerHelper();
        externalFileSerializerHelper = new ExternalFileSerializerHelper();
        remoteSerializerHelper = new RemoteSerializerHelper();
    }

    /*
     * Controller -> Satellite
     */
    @Override
    public ProtoCtrlStltSerializerBuilder authMessage(
        UUID nodeUuid,
        String nodeName,
        byte[] sharedSecret,
        UUID ctrlUuid
    )
    {
        try
        {
            MsgIntAuthOuterClass.MsgIntAuth.newBuilder()
                .setNodeUuid(nodeUuid.toString())
                .setNodeName(nodeName)
                .setSharedSecret(ByteString.copyFrom(sharedSecret))
                .setCtrlUuid(ctrlUuid.toString())
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public @Nonnull CtrlStltSerializerBuilder changedConfig(@Nonnull com.linbit.linstor.core.cfg.StltConfig stltConfig)
        throws IOException
    {
        StltConfigOuterClass.StltConfig.Builder bld = stltConfig(
            stltConfig.getConfigDir(),
            stltConfig.isDebugConsoleEnabled(),
            stltConfig.isLogPrintStackTrace(),
            stltConfig.getLogDirectory(),
            stltConfig.getLogLevel(),
            stltConfig.getLogLevelLinstor(),
            stltConfig.getStltOverrideNodeName(),
            stltConfig.isRemoteSpdk(),
            stltConfig.isEbs(),
            stltConfig.getDrbdKeepResPattern(),
            stltConfig.getNetBindAddress(),
            stltConfig.getNetPort(),
            stltConfig.getNetType(),
            SetUtils.convertPathsToStrings(stltConfig.getWhitelistedExternalFilePaths())
        );
        bld.build().writeDelimitedTo(baos);
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder stltConfigApplied(boolean success) throws IOException
    {
        MsgIntApplyConfigResponse.newBuilder().setSuccess(success).build().writeDelimitedTo(baos);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder changedData(AtomicUpdateSatelliteData data)
    {
        try
        {
            MsgIntChangedData.Builder builder = MsgIntChangedData.newBuilder();

            for (ResourceDefinition rscDfn : data.getRscDfnsToUpdate())
            {
                builder.addRscs(
                    ChangedResource.newBuilder()
                        .setUuid(rscDfn.getUuid().toString())
                        .setName(rscDfn.getName().displayValue)
                        .build()
                );
            }
            for (SnapshotDefinition snapDfn : data.getSnapDfnsToUpdate())
            {
                builder.addSnaps(
                    ChangedSnapshot.newBuilder()
                        .setUuid(snapDfn.getUuid().toString())
                        .setRscName(snapDfn.getResourceName().displayValue)
                        .setSnapName(snapDfn.getName().displayValue)
                        .build()
                );
            }
            // TODO add nodes, storpools, ...

            builder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    // no fullSync- or update-id needed
    @Override
    public ProtoCtrlStltSerializerBuilder changedNode(UUID nodeUuid, String nodeName)
    {
        appendObjectId(nodeUuid, nodeName);
        return this;
    }

    // no fullSync- or update-id needed
    @Override
    public ProtoCtrlStltSerializerBuilder changedResource(UUID rscUuid, String rscName)
    {
        appendObjectId(rscUuid, rscName);
        return this;
    }

    // no fullSync- or update-id needed
    @Override
    public ProtoCtrlStltSerializerBuilder changedStorPool(UUID storPoolUuid, String storPoolName)
    {
        appendObjectId(storPoolUuid, storPoolName);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder changedSnapshot(
        String rscName,
        UUID snapshotUuid,
        String snapshotName
    )
    {
        appendObjectId(null, rscName);
        appendObjectId(snapshotUuid, snapshotName);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder changedExtFile(
        UUID extFileUuidRef,
        String extFileNameRef
    )
    {
        appendObjectId(extFileUuidRef, extFileNameRef);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder changedRemote(
        UUID remoteUuidRef,
        String remoteNameRef
    )
    {
        appendObjectId(remoteUuidRef, remoteNameRef);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder controllerData(
        long fullSyncTimestamp,
        long serializerid
    )
    {
        try
        {
            ctrlSerializerHelper
                .buildApplyControllerMsg(fullSyncTimestamp, serializerid)
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder node(
        Node node,
        Collection<Node> relatedNodes,
        long fullSyncTimestamp,
        long serializerId
    )
    {
        try
        {
            MsgIntApplyNode.newBuilder()
                .setNode(nodeSerializerHelper.buildNodeMsg(node, relatedNodes))
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(serializerId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder deletedNode(
        String nodeNameStr,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            MsgIntApplyDeletedNode.newBuilder()
                .setNodeName(nodeNameStr)
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder resource(
        Resource localResource,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            MsgIntApplyRsc.newBuilder()
                .setRsc(rscSerializerHelper.buildIntResource(localResource))
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder deletedResource(
        String rscNameStr,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            MsgIntApplyDeletedRsc.newBuilder()
                .setRscName(rscNameStr)
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder storPool(
        StorPool storPool,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            MsgIntApplyStorPool.newBuilder()
                .setStorPool(buildIntStorPoolMsg(storPool))
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder deletedStorPool(
        String storPoolNameStr,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            MsgIntApplyDeletedStorPool.newBuilder()
                .setStorPoolName(storPoolNameStr)
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder snapshot(
        Snapshot snapshot,
        long fullSyncId,
        long updateId
    )
    {
        try
        {
            MsgIntApplySnapshot.newBuilder()
                .setSnapshot(snapshotSerializerHelper.buildSnapshotMsg(snapshot))
                .setFullSyncId(fullSyncId)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder endedSnapshot(
        String resourceNameStr,
        String snapshotNameStr,
        long fullSyncId,
        long updateId
    )
    {
        try
        {
            MsgIntSnapshotEndedDataOuterClass.MsgIntSnapshotEndedData.newBuilder()
                .setRscName(resourceNameStr)
                .setSnapshotName(snapshotNameStr)
                .setFullSyncId(fullSyncId)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder fullSync(
        Set<Node> nodeSet,
        Set<StorPool> storPools,
        Set<Resource> resources,
        Set<Snapshot> snapshots,
        Set<ExternalFile> externalFiles,
        Set<AbsRemote> remotes,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            ArrayList<IntNode> serializedNodes = new ArrayList<>();
            ArrayList<IntStorPool> serializedStorPools = new ArrayList<>();
            ArrayList<IntRsc> serializedRscs = new ArrayList<>();
            ArrayList<IntSnapshot> serializedSnapshots = new ArrayList<>();
            ArrayList<IntExternalFile> serializedExtFiles = new ArrayList<>();
            ArrayList<IntS3Remote> serializedS3Remotes = new ArrayList<>();
            ArrayList<IntEbsRemote> serializedEbsRemotes = new ArrayList<>();

            IntController serializedCtrl = ctrlSerializerHelper.buildControllerDataMsg();

            LinkedList<Node> nodes = new LinkedList<>(nodeSet);

            while (!nodes.isEmpty())
            {
                Node node = nodes.removeFirst();
                serializedNodes.add(
                    nodeSerializerHelper.buildNodeMsg(node, nodes)
                );
            }
            for (StorPool storPool : storPools)
            {
                serializedStorPools.add(
                    buildIntStorPoolMsg(storPool)
                );
            }
            for (Resource rsc : resources)
            {
                if (rsc.iterateVolumes().hasNext())
                {
                    serializedRscs.add(rscSerializerHelper.buildIntResource(rsc));
                }
            }
            for (Snapshot snapshot : snapshots)
            {
                serializedSnapshots.add(snapshotSerializerHelper.buildSnapshotMsg(snapshot));
            }
            for (ExternalFile extFile : externalFiles)
            {
                serializedExtFiles.add(externalFileSerializerHelper.buildExtFileMsg(extFile, false));
            }
            for (AbsRemote remote : remotes)
            {
                if (remote instanceof S3Remote)
                {
                    serializedS3Remotes.add(remoteSerializerHelper.buildS3RemoteMsg((S3Remote) remote));
                }
                else if (remote instanceof EbsRemote)
                {
                    serializedEbsRemotes.add(remoteSerializerHelper.buildEbsRemoteMsg((EbsRemote) remote));
                }
                else if (remote instanceof StltRemote)
                {
                    // should never happen, can be ignored
                }
            }

            MsgIntApplyFullSync.Builder builder = MsgIntApplyFullSync.newBuilder()
                .addAllNodes(serializedNodes)
                .addAllStorPools(serializedStorPools)
                .addAllRscs(serializedRscs)
                .addAllSnapshots(serializedSnapshots)
                .addAllExternalFiles(serializedExtFiles)
                .addAllEbsRemotes(serializedEbsRemotes)
                .addAllS3Remotes(serializedS3Remotes)
                .setFullSyncTimestamp(fullSyncTimestamp)
                .setCtrl(serializedCtrl);

            if (secObjs.areAllSet())
            {
                builder.setMasterKey(Base64.encode(secObjs.getCryptKey()))
                    .setCryptHash(ByteString.copyFrom(secObjs.getCryptHash()))
                    .setCryptSalt(ByteString.copyFrom(secObjs.getCryptSalt()))
                    .setEncCryptKey(ByteString.copyFrom(secObjs.getEncKey()));
            }
            builder.build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder remote(
        AbsRemote remoteRef,
        long fullSyncIdRef,
        long updateIdRef
    )
    {
        try
        {
            MsgIntApplyRemote.Builder builder = MsgIntApplyRemote.newBuilder();
            if (remoteRef instanceof S3Remote)
            {
                builder.setS3Remote(remoteSerializerHelper.buildS3RemoteMsg((S3Remote) remoteRef));
            }
            else if (remoteRef instanceof StltRemote)
            {
                builder.setSatelliteRemote(remoteSerializerHelper.buildStltRemoteMsg((StltRemote) remoteRef));
            }
            else if (remoteRef instanceof EbsRemote)
            {
                builder.setEbsRemote(remoteSerializerHelper.buildEbsRemoteMsg((EbsRemote) remoteRef));
            }
            else
            {
                throw new ImplementationError(
                    "Cannot serialize unknown Remote type: " +
                        (remoteRef == null ? "null" : remoteRef.getClass().getSimpleName())
                );
            }
            builder.setFullSyncId(fullSyncIdRef)
                .setUpdateId(updateIdRef)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder deletedRemote(
        String remoteNameStrRef,
        long fullSyncIdRef,
        long updateIdRef
    )
    {
        try
        {
            MsgIntRemoteDeleted.newBuilder()
                .setRemoteName(remoteNameStrRef)
                .setFullSyncId(fullSyncIdRef)
                .setUpdateId(updateIdRef)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder grantsharedStorPoolLocks(Set<SharedStorPoolName> locksRef)
    {
        try
        {
            MsgIntApplySharedStorPoolLocks.Builder builder = MsgIntApplySharedStorPoolLocks.newBuilder();
            for (SharedStorPoolName lock : locksRef)
            {
                builder.addSharedStorPoolLocks(lock.displayValue);
            }
            builder.build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder externalFile(
        ExternalFile extFileRef,
        boolean includeContent,
        long fullSyncIdRef,
        long updateIdRef
    )
    {
        try
        {
            MsgIntApplyExternalFile.newBuilder()
                .setExternalFile(externalFileSerializerHelper.buildExtFileMsg(extFileRef, includeContent))
                .setFullSyncId(fullSyncIdRef)
                .setUpdateId(updateIdRef)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder deletedExternalFile(String extFileNameStrRef, long fullSyncIdRef, long updateIdRef)
    {
        try
        {
            MsgIntExternalFileDeletedData.newBuilder()
                .setExternalFileName(extFileNameStrRef)
                .setFullSyncId(fullSyncIdRef)
                .setUpdateId(updateIdRef)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    /*
     * Satellite -> Controller
     */
    @Override
    public ProtoCtrlStltSerializerBuilder primaryRequest(String rscName, String rscUuid, boolean alreadyInitialized)
    {
        try
        {
            MsgIntPrimaryOuterClass.MsgIntPrimary.newBuilder()
                .setRscName(rscName)
                .setRscUuid(rscUuid)
                .setAlreadyInitialized(alreadyInitialized)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder notifyNodeApplied(Node node)
    {
        try
        {
            MsgIntApplyNodeSuccessOuterClass.MsgIntApplyNodeSuccess.newBuilder()
                .setNodeId(
                    IntObjectId.newBuilder()
                        .setUuid(node.getUuid().toString())
                        .setName(node.getName().displayValue)
                        .build()
                )
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder updateLocalProps(LocalPropsChangePojo pojoRef)
    {
        try
        {
            MsgIntUpdateLocalNodeChangeOuterClass.MsgIntUpdateLocalNodeChange.Builder builder = MsgIntUpdateLocalNodeChangeOuterClass.MsgIntUpdateLocalNodeChange.newBuilder();
            builder.setChangedNodeProps(mapToProps(pojoRef.changedNodeProps))
                .setDeletedNodeProps(setToSet(pojoRef.deletedNodeProps));
            for (Entry<StorPoolName, Map<String, String>> entry : pojoRef.changedStorPoolProps.entrySet())
            {
                builder.putChangedStorPoolProps(entry.getKey().displayValue, mapToProps(entry.getValue()));
            }
            for (Entry<StorPoolName, Set<String>> entry : pojoRef.deletedStorPoolProps.entrySet())
            {
                builder.putDeletedStorPoolProps(entry.getKey().displayValue, setToSet(entry.getValue()));
            }
            builder
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    private MsgIntUpdateLocalNodeChangeOuterClass.Props mapToProps(Map<String, String> map)
    {
        return MsgIntUpdateLocalNodeChangeOuterClass.Props.newBuilder().putAllProps(map).build();
    }

    private MsgIntUpdateLocalNodeChangeOuterClass.Set setToSet(Set<String> set)
    {
        return MsgIntUpdateLocalNodeChangeOuterClass.Set.newBuilder().addAllKeys(set).build();
    }

    @Override
    public ProtoCtrlStltSerializerBuilder notifyResourceApplied(
        Resource resource,
        Map<StorPool, SpaceInfo> freeSpaceMap
    )
    {
        try
        {
            MsgIntApplyRscSuccess.Builder builder = MsgIntApplyRscSuccessOuterClass.MsgIntApplyRscSuccess.newBuilder()
                .setRscId(
                    IntObjectId.newBuilder()
                        .setUuid(resource.getUuid().toString())
                        .setName(resource.getResourceDefinition().getName().displayValue)
                        .build()
                )
                .addAllFreeSpace(
                    ProtoStorPoolFreeSpaceUtils.getAllStorPoolFreeSpaces(freeSpaceMap)
                )
                .setLayerObject(
                    ProtoCommonSerializerBuilder.LayerObjectSerializer.serializeLayerObject(
                        resource.getLayerData(serializerCtx),
                        serializerCtx
                    )
                )
                .putAllRscProps(resource.getProps(serializerCtx).map());

            for (Volume vlm : resource.streamVolumes().collect(Collectors.toList()))
            {
                builder.putVlmProps(
                    vlm.getVolumeNumber().value,
                    MsgIntApplyRscSuccessOuterClass.Props.newBuilder()
                        .putAllProp(vlm.getProps(serializerCtx).map())
                        .build()
                );
            }

            final NodeName localNodeName = resource.getNode().getName();
            for (SnapshotDefinition snapDfn : resource.getResourceDefinition().getSnapshotDfns(serializerCtx))
            {
                Snapshot snap = snapDfn.getSnapshot(serializerCtx, localNodeName);
                String snapName = snapDfn.getName().value;
                builder.putSnapProps(
                    snapName,
                    MsgIntApplyRscSuccessOuterClass.Props.newBuilder()
                        .putAllProp(snap.getProps(serializerCtx).map())
                        .build()
                );
                builder.putSnapStorageLayerObjects(
                    snapName,
                    ProtoCommonSerializerBuilder.LayerObjectSerializer.serializeLayerObject(
                        snap.getLayerData(serializerCtx),
                        serializerCtx
                    )
                );

                MsgIntApplyRscSuccessOuterClass.SnapVlmProps.Builder allSnapVlmPropsBuilder =
                    MsgIntApplyRscSuccessOuterClass.SnapVlmProps.newBuilder();
                for (SnapshotVolumeDefinition snapVlmDfn : snapDfn.getAllSnapshotVolumeDefinitions(serializerCtx))
                {
                    VolumeNumber vlmNr = snapVlmDfn.getVolumeNumber();
                    SnapshotVolume snapVlm = snap.getVolume(vlmNr);

                    allSnapVlmPropsBuilder.putSnapVlmProps(
                        vlmNr.value,
                        MsgIntApplyRscSuccessOuterClass.Props.newBuilder()
                            .putAllProp(snapVlm.getProps(serializerCtx).map())
                            .build()
                    );

                }
                builder.putSnapVlmProps(snapName, allSnapVlmPropsBuilder.build());
            }

            builder
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder notifySnapshotShipped(
        Snapshot snapRef,
        boolean success,
        Set<Integer> vlmNrsWithBlockedPort
    )
    {
        try
        {
            MsgIntSnapshotShipped.newBuilder()
                .setRscName(snapRef.getResourceName().displayValue)
                .setSnapName(snapRef.getSnapshotName().displayValue)
                .setSuccess(success)
                .addAllVlmNrsWithBlockedPort(vlmNrsWithBlockedPort)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder notifyCloneUpdate(String rscName, int vlmNr, boolean successRef)
    {
        try
        {
            MsgIntCloneUpdate.newBuilder()
                .setRscName(rscName)
                .setVlmNr(vlmNr)
                .setSuccess(successRef)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder notifyBackupShipped(
        SnapshotDefinition.Key snapKeyRef,
        boolean success,
        Set<Integer> ports
    )
    {
        try
        {
            MsgIntBackupShipped.newBuilder()
                .setRscName(snapKeyRef.getResourceName().displayValue)
                .setSnapName(snapKeyRef.getSnapshotName().displayValue)
                .addAllPorts(ports)
                .setSuccess(success)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder notifyBackupShippingWrongPorts(
        String remoteName,
        String snapName,
        String rscName,
        Set<Integer> ports
    )
    {
        try
        {
            MsgIntBackupShippingWrongPorts.newBuilder()
                .setRemoteName(remoteName)
                .setSnapName(snapName)
                .setRscName(rscName)
                .addAllPorts(ports)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder notifyBackupRcvReady(
        String remoteName,
        String snapName,
        String rscName,
        String nodeName
    )
    {
        try
        {
            MsgIntBackupRcvReady.newBuilder()
                .setRemoteName(remoteName)
                .setSnapName(snapName)
                .setRscName(rscName)
                .setNodeName(nodeName)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder notifyBackupShippingId(
        Snapshot snapRef,
        String backupName,
        String uploadId,
        String remoteNameRef
    )
    {
        try
        {
            MsgIntBackupShippingId.newBuilder()
                .setNodeName(snapRef.getNodeName().displayValue)
                .setRscName(snapRef.getResourceName().displayValue)
                .setSnapName(snapRef.getSnapshotName().displayValue)
                .setBackupName(backupName)
                .setUploadId(uploadId)
                .setRemoteName(remoteNameRef)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder notifyBackupShippingFinished(String rscName, String snapName)
    {
        try
        {
            MsgIntBackupShippingFinished.newBuilder()
                .setRscName(rscName)
                .setSnapName(snapName)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder notifyResourceFailed(Resource resource, ApiCallRc apiCallRc)
    {
        try
        {
            MsgRscFailed.Builder builder = MsgRscFailed.newBuilder()
                .setRsc(ProtoCommonSerializerBuilder.serializeResource(serializerCtx, resource))
                .addAllResponses(ProtoCommonSerializerBuilder.serializeApiCallRc(apiCallRc));

            builder.putAllSnapStorageLayerObjects(createSnapLayerMap(resource))
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    private Map<String, RscLayerData> createSnapLayerMap(Resource resource) throws AccessDeniedException
    {
        // <SnapName, RscLayerData>
        Map<String, RscLayerData> ret = new HashMap<>();
        final NodeName localNodeName = resource.getNode().getName();
        for (SnapshotDefinition snapDfn : resource.getResourceDefinition().getSnapshotDfns(serializerCtx))
        {
            Snapshot snap = snapDfn.getSnapshot(serializerCtx, localNodeName);
            String snapName = snapDfn.getName().value;
            ret.put(
                snapName,
                ProtoCommonSerializerBuilder.LayerObjectSerializer.serializeLayerObject(
                    snap.getLayerData(serializerCtx),
                    serializerCtx
                )
            );
        }
        return ret;
    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder notifySnapshotRollbackResult(
        Resource rscRef,
        ApiCallRc apiCallRcRef,
        boolean successRef
    )
    {
        try
        {
            MsgSnapshotRollbackResult.newBuilder()
                .setRsc(ProtoCommonSerializerBuilder.serializeResource(serializerCtx, rscRef))
                .addAllResponses(ProtoCommonSerializerBuilder.serializeApiCallRc(apiCallRcRef))
                .setSuccess(successRef)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        catch (AccessDeniedException exc)
        {
            handleAccessDeniedException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder requestNodeUpdate(UUID nodeUuid, String nodeName)
    {
        appendObjectId(nodeUuid, nodeName);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder requestControllerUpdate()
    {
        return requestNodeUpdate(UUID.randomUUID(), "thisnamedoesnotmatter");
    }

    @Override
    public ProtoCtrlStltSerializerBuilder requestResourceUpdate(UUID rscUuid, String nodeName, String rscName)
    {
        appendObjectId(null, nodeName);
        appendObjectId(rscUuid, rscName);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder requestStoragePoolUpdate(UUID storPoolUuid, String storPoolName)
    {
        appendObjectId(storPoolUuid, storPoolName);
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder requestSnapshotUpdate(
        String rscName,
        UUID snapshotUuid,
        String snapshotName
    )
    {
        appendObjectId(null, rscName);
        appendObjectId(snapshotUuid, snapshotName);
        return this;
    }

    @Override
    public CtrlStltSerializerBuilder requestSharedStorPoolLocks(Set<SharedStorPoolName> sharedSPLocksRef)
    {
        try
        {
            Set<String> stringSet = new TreeSet<>();
            for (SharedStorPoolName lock : sharedSPLocksRef)
            {
                stringSet.add(lock.displayValue);
            }
            MsgIntRequestSharedStorPoolLocks.newBuilder()
                .addAllSharedStorPoolLocks(stringSet)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder requestExternalFileUpdate(
        UUID externalFileUuidRef,
        String externalFileNameRef
    )
    {
        appendObjectId(externalFileUuidRef, externalFileNameRef);
        return this;
    }

    @Override
    public CommonSerializerBuilder requestRemoteUpdate(
        UUID remoteUuidRef,
        String remoteNameRef
    )
    {
        appendObjectId(remoteUuidRef, remoteNameRef);
        return this;
    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder updateFreeCapacities(
        Map<StorPool, SpaceInfo> spaceInfoMap
    )
    {
        try
        {
            List<StorPoolFreeSpace> freeSpaces = new ArrayList<>();
            for (Entry<StorPool, SpaceInfo> entry : spaceInfoMap.entrySet())
            {
                StorPool storPool = entry.getKey();
                SpaceInfo spaceInfo = entry.getValue();
                freeSpaces.add(
                    StorPoolFreeSpaceOuterClass.StorPoolFreeSpace.newBuilder()
                        .setStorPoolUuid(storPool.getUuid().toString())
                        .setStorPoolName(storPool.getName().displayValue)
                        .setFreeCapacity(spaceInfo.freeCapacity)
                        .setTotalCapacity(spaceInfo.totalCapacity)
                        .build()
                );
            }

            MsgIntUpdateFreeSpace.newBuilder()
                .addAllFreeSpace(freeSpaces)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder storPoolApplied(
        StorPool storPool,
        SpaceInfo spaceInfo,
        boolean supportsSnapshotsRef
    )
    {
        try
        {
            MsgIntApplyStorPoolSuccess.newBuilder()
                .setStorPoolName(storPool.getName().displayValue)
                .setFreeSpace(
                    StorPoolFreeSpaceOuterClass.StorPoolFreeSpace.newBuilder()
                        .setStorPoolUuid(storPool.getUuid().toString())
                        .setStorPoolName(storPool.getName().displayValue)
                        .setFreeCapacity(spaceInfo.freeCapacity)
                        .setTotalCapacity(spaceInfo.totalCapacity)
                        .build()
                )
                .setSupportsSnapshots(supportsSnapshotsRef)
                .setIsPmem(storPool.isPmem())
                .setIsVdo(storPool.isVDO())
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;

    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder requestPhysicalDevices(boolean filter)
    {
        try
        {
            MsgReqPhysicalDevices.newBuilder()
                .setFilter(filter)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder createDevicePool(
        List<String> devicePaths,
        DeviceProviderKind providerKindRef,
        RaidLevel raidLevel,
        String poolName,
        boolean vdoEnabled,
        long vdoLogicalSizeKib,
        long vdoSlabSize,
        boolean sed,
        String sedPassword
    )
    {
        try
        {
            MsgCreateDevicePool.Builder msgCreateDevicePoolBuilder = MsgCreateDevicePool.newBuilder()
                .addAllDevicePaths(devicePaths)
                .setProviderKind(asProviderType(providerKindRef))
                .setRaidLevel(MsgCreateDevicePool.RaidLevel.valueOf(raidLevel.name()))
                .setPoolName(poolName)
                .setLogicalSizeKib(vdoLogicalSizeKib)
                .setSed(sed)
                .setSedPassword(sedPassword);

            if (vdoEnabled)
            {
                msgCreateDevicePoolBuilder.setVdoArguments(
                    MsgCreateDevicePoolOuterClass.VdoArguments.newBuilder()
                        .setSlabSizeKib(vdoSlabSize)
                        .build()
                );
            }

            msgCreateDevicePoolBuilder
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder deleteDevicePool(
        List<String> devicePaths,
        DeviceProviderKind providerKindRef,
        String poolName
    )
    {
        try
        {
            MsgDeleteDevicePool.Builder msgDeleteDevicePoolBuilder = MsgDeleteDevicePool.newBuilder()
                .addAllDevicePaths(devicePaths)
                .setProviderKind(asProviderType(providerKindRef))
                .setPoolName(poolName);

            msgDeleteDevicePoolBuilder
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlStltSerializer.CtrlStltSerializerBuilder physicalDevices(List<LsBlkEntry> entries)
    {
        try
        {
            MsgPhysicalDevices.newBuilder()
                .addAllDevices(
                    entries.stream().map(
                        lsBlkEntry -> MsgPhysicalDevicesOuterClass.LsBlkEntry.newBuilder()
                            .setName(lsBlkEntry.getName())
                            .setSize(lsBlkEntry.getSize())
                            .setRotational(lsBlkEntry.isRotational())
                            .setKernelName(lsBlkEntry.getKernelName())
                            .setParentName(lsBlkEntry.getParentName())
                            .setMajor(lsBlkEntry.getMajor())
                            .setMinor(lsBlkEntry.getMinor())
                            .build()
                    ).collect(Collectors.toList())
                )
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlStltSerializerBuilder cryptKey(
        byte[] cryptKey,
        byte[] cryptHash,
        byte[] cryptSalt,
        byte[] encKey,
        long fullSyncTimestamp,
        long updateId
    )
    {
        try
        {
            MsgIntCryptKey.newBuilder()
                .setCryptKey(ByteString.copyFrom(cryptKey))
                .setCryptHash(ByteString.copyFrom(cryptHash))
                .setCryptSalt(ByteString.copyFrom(cryptSalt))
                .setEncCryptKey(ByteString.copyFrom(encKey))
                .setFullSyncId(fullSyncTimestamp)
                .setUpdateId(updateId)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    private void appendObjectId(UUID objUuid, String objName)
    {
        try
        {
            IntObjectId.Builder msgBuilder = IntObjectId.newBuilder();
            if (objUuid != null)
            {
                msgBuilder.setUuid(objUuid.toString());
            }
            msgBuilder
                .setName(objName)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
    }

    private IntStorPool buildIntStorPoolMsg(StorPool storPool)
        throws AccessDeniedException
    {
        StorPoolDefinition storPoolDfn = storPool.getDefinition(serializerCtx);
        return IntStorPool.newBuilder()
            .setStorPool(ProtoCommonSerializerBuilder.serializeStorPool(serializerCtx, storPool))
            .setStorPoolDfn(ProtoCommonSerializerBuilder.serializeStorPoolDfn(serializerCtx, storPoolDfn))
            .build();
    }

    private class NodeSerializerHelper
    {
        private IntNode buildNodeMsg(Node node, Collection<Node> relatedNodes)
            throws AccessDeniedException
        {
            return IntNode.newBuilder()
                .setUuid(node.getUuid().toString())
                .setName(node.getName().displayValue)
                .setFlags(node.getFlags().getFlagsBits(serializerCtx))
                .setType(node.getNodeType(serializerCtx).name())
                .addAllNetIfs(
                    getNetIfs(node)
                )
                .addAllNodeConns(
                    getNodeConns(node, relatedNodes)
                )
                .putAllProps(
                    node.getProps(serializerCtx).map()
                )
                .build();
        }

        private Iterable<? extends IntNetIf> getNetIfs(Node node) throws AccessDeniedException
        {
            ArrayList<IntNetIf> netIfs = new ArrayList<>();
            for (NetInterface netIf : node.streamNetInterfaces(serializerCtx).collect(toList()))
            {
                Builder builder = IntNetIf.newBuilder()
                    .setUuid(netIf.getUuid().toString())
                    .setName(netIf.getName().displayValue)
                    .setAddr(netIf.getAddress(serializerCtx).getAddress());
                if (netIf.getStltConnPort(serializerCtx) != null)
                {
                    builder.setStltConnPort(netIf.getStltConnPort(serializerCtx).value);
                }
                if (netIf.getStltConnEncryptionType(serializerCtx) != null)
                {
                    builder.setStltConnEncrType(netIf.getStltConnEncryptionType(serializerCtx).name());
                }
                netIfs.add(builder.build());
            }
            return netIfs;
        }

        private ArrayList<IntNodeConn> getNodeConns(Node node, Collection<Node> otherNodes)
            throws AccessDeniedException
        {
            ArrayList<IntNodeConn> nodeConns = new ArrayList<>();
            for (Node otherNode : otherNodes)
            {
                NodeConnection nodeConnection = node.getNodeConnection(serializerCtx, otherNode);

                if (nodeConnection != null)
                {
                    nodeConns.add(
                        IntNodeConn.newBuilder()
                            .setUuid(nodeConnection.getUuid().toString())
                            .setOtherNode(buildNodeMsg(otherNode, Collections.emptyList()))
                            .putAllProps(nodeConnection.getProps(serializerCtx).map())
                            .build()
                    );
                }
            }
            return nodeConns;
        }
    }

    private class ExternalFileSerializerHelper
    {
        private IntExternalFile buildExtFileMsg(ExternalFile extFile, boolean includeContentRef)
            throws AccessDeniedException
        {
            IntExternalFile.Builder builder = IntExternalFile.newBuilder()
                .setUuid(extFile.getUuid().toString())
                .setName(extFile.getName().extFileName)
                .setFlags(extFile.getFlags().getFlagsBits(serializerCtx))
                .setContentChecksum(ByteString.copyFrom(extFile.getContentCheckSum(serializerCtx)));
            if (includeContentRef)
            {
                builder = builder.setContent(ByteString.copyFrom(extFile.getContent(serializerCtx)));
            }
            return builder
                .build();
        }
    }

    private class RemoteSerializerHelper
    {
        private IntS3Remote buildS3RemoteMsg(S3Remote s3remote)
            throws AccessDeniedException
        {
            IntS3Remote.Builder builder = IntS3Remote.newBuilder()
                .setUuid(s3remote.getUuid().toString())
                .setName(s3remote.getName().displayValue)
                .setFlags(s3remote.getFlags().getFlagsBits(serializerCtx))
                .setEndpoint(s3remote.getUrl(serializerCtx))
                .setBucket(s3remote.getBucket(serializerCtx))
                .setRegion(s3remote.getRegion(serializerCtx))
                .setAccessKey(ByteString.copyFrom(s3remote.getAccessKey(serializerCtx)))
                .setSecretKey(ByteString.copyFrom(s3remote.getSecretKey(serializerCtx)));
            return builder
                .build();
        }

        private IntEbsRemote buildEbsRemoteMsg(EbsRemote ebsremote)
            throws AccessDeniedException
        {
            IntEbsRemote.Builder builder = IntEbsRemote.newBuilder()
                .setUuid(ebsremote.getUuid().toString())
                .setName(ebsremote.getName().displayValue)
                .setFlags(ebsremote.getFlags().getFlagsBits(serializerCtx))
                .setUrl(ebsremote.getUrl(serializerCtx).toString())
                .setAvailabilityZone(ebsremote.getAvailabilityZone(serializerCtx))
                .setRegion(ebsremote.getRegion(serializerCtx))
                .setAccessKey(ByteString.copyFrom(ebsremote.getEncryptedAccessKey(serializerCtx)))
                .setSecretKey(ByteString.copyFrom(ebsremote.getEncryptedSecretKey((serializerCtx))));
            return builder
                .build();
        }

        public IntStltRemote buildStltRemoteMsg(StltRemote stltremote)
            throws AccessDeniedException
        {
            IntStltRemote.Builder builder = IntStltRemote.newBuilder()
                .setUuid(stltremote.getUuid().toString())
                .setName(stltremote.getName().displayValue)
                .setFlags(stltremote.getFlags().getFlagsBits(serializerCtx));

            String ip = stltremote.getIp(serializerCtx);
            if (ip != null)
            {
                builder.setTargetIp(ip);
            }
            Map<String, Integer> ports = stltremote.getPorts(serializerCtx);
            if (ports != null && !ports.isEmpty())
            {
                builder.putAllTargetPorts(ports);
            }
            Boolean useZstd = stltremote.useZstd(serializerCtx);
            if (useZstd != null)
            {
                builder.setUseZstd(useZstd);
            }
            return builder.build();
        }
    }

    private class CtrlSerializerHelper
    {
        private ReadOnlyProps ctrlConfProps;

        CtrlSerializerHelper(
            final ReadOnlyProps ctrlConfRef
        )
        {
            ctrlConfProps = ctrlConfRef;
        }

        private MsgIntApplyController buildApplyControllerMsg(long fullSyncTimeStamp, long updateId)
        {
            return MsgIntApplyController.newBuilder()
                .setCtrl(buildControllerDataMsg())
                .setFullSyncId(fullSyncTimeStamp)
                .setUpdateId(updateId)
                .build();
        }

        private IntController buildControllerDataMsg()
        {
            return IntController.newBuilder()
                .putAllProps(ctrlConfProps.map())
                .build();
        }
    }

    private class ResourceSerializerHelper
    {
        private IntRsc buildIntResource(Resource localResource)
            throws AccessDeniedException
        {
            List<Resource> otherResources = new ArrayList<>();
            Iterator<Resource> rscIterator = localResource.getResourceDefinition().iterateResource(serializerCtx);
            while (rscIterator.hasNext())
            {
                Resource rsc = rscIterator.next();
                if (!rsc.equals(localResource))
                {
                    otherResources.add(rsc);
                }
            }

            ResourceDefinition rscDfn = localResource.getResourceDefinition();

            return IntRsc.newBuilder()
                .setLocalRsc(ProtoCommonSerializerBuilder.serializeResource(serializerCtx, localResource))
                .setRscDfn(ProtoCommonSerializerBuilder.serializeResourceDefinition(serializerCtx, rscDfn))
                .addAllOtherResources(buildOtherResources(otherResources))
                .addAllRscConnections(
                    ProtoCommonSerializerBuilder.serializeResourceConnections(
                        serializerCtx,
                        localResource.streamAbsResourceConnections(serializerCtx).collect(Collectors.toList())
                    )
                )
                .build();
        }

        private List<IntOtherRsc> buildOtherResources(List<Resource> otherResources)
            throws AccessDeniedException
        {
            List<IntOtherRsc> list = new ArrayList<>();

            for (Resource rsc : otherResources)
            {
                list.add(
                    IntOtherRsc.newBuilder()
                        .setNode(
                            ProtoCommonSerializerBuilder.serializeNode(
                                serializerCtx,
                                rsc.getNode()
                            )
                        )
                        .setRsc(ProtoCommonSerializerBuilder.serializeResource(serializerCtx, rsc))
                        .build()
                );
            }

            return list;
        }
    }

    private class SnapshotSerializerHelper
    {
        private IntSnapshot buildSnapshotMsg(Snapshot snapshot)
            throws AccessDeniedException
        {
            SnapshotDefinition snapshotDfn = snapshot.getSnapshotDefinition();
            ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();

            List<IntSnapshotOuterClass.SnapshotVlmDfn> snapshotVlmDfns = new ArrayList<>();
            for (
                SnapshotVolumeDefinition snapshotVolumeDefinition : snapshotDfn
                    .getAllSnapshotVolumeDefinitions(serializerCtx)
            )
            {
                snapshotVlmDfns.add(
                    IntSnapshotOuterClass.SnapshotVlmDfn.newBuilder()
                        .setSnapshotVlmDfnUuid(snapshotVolumeDefinition.getUuid().toString())
                        .setVlmNr(snapshotVolumeDefinition.getVolumeNumber().value)
                        .setVlmSize(snapshotVolumeDefinition.getVolumeSize(serializerCtx))
                        .setFlags(snapshotVolumeDefinition.getFlags().getFlagsBits(serializerCtx))
                        .putAllSnapshotVlmDfnProps(snapshotVolumeDefinition.getProps(serializerCtx).map())
                        .build()
                );
            }

            List<IntSnapshotOuterClass.SnapshotVlm> snapshotVlms = new ArrayList<>();
            Iterator<SnapshotVolume> snapVlmIt = snapshot.iterateVolumes();
            while (snapVlmIt.hasNext())
            {
                SnapshotVolume snapshotVolume = snapVlmIt.next();
                IntSnapshotOuterClass.SnapshotVlm.Builder builder = IntSnapshotOuterClass.SnapshotVlm
                    .newBuilder()
                    .setSnapshotVlmUuid(snapshotVolume.getUuid().toString())
                    .setSnapshotVlmDfnUuid(snapshotDfn.getUuid().toString())
                    .setVlmNr(snapshotVolume.getVolumeNumber().value)
                    .putAllSnapshotVlmProps(snapshotVolume.getProps(serializerCtx).map());
                final String state = snapshotVolume.getState(serializerCtx);
                if (state != null)
                {
                    builder.setState(state);
                }
                snapshotVlms.add(builder.build());
            }

            return IntSnapshot.newBuilder()
                .setNodeName(snapshot.getNodeName().displayValue)
                .setRscName(rscDfn.getName().displayValue)
                .setRscDfnUuid(rscDfn.getUuid().toString())
                .setRscDfnFlags(rscDfn.getFlags().getFlagsBits(serializerCtx))
                .setRscGrp(serializeResourceGroup(serializerCtx, rscDfn.getResourceGroup()))
                .putAllRscDfnProps(rscDfn.getProps(serializerCtx).map())
                .setSnapshotUuid(snapshot.getUuid().toString())
                .setSnapshotName(snapshotDfn.getName().displayValue)
                .setSnapshotDfnUuid(snapshotDfn.getUuid().toString())
                .addAllSnapshotVlmDfns(snapshotVlmDfns)
                .setSnapshotDfnFlags(snapshotDfn.getFlags().getFlagsBits(serializerCtx))
                .putAllSnapshotDfnProps(snapshotDfn.getProps(serializerCtx).map())
                .putAllSnapshotProps(snapshot.getProps(serializerCtx).map())
                .addAllSnapshotVlms(snapshotVlms)
                .setFlags(snapshot.getFlags().getFlagsBits(serializerCtx))
                .setSuspendResource(snapshot.getSuspendResource(serializerCtx))
                .setTakeSnapshot(snapshot.getTakeSnapshot(serializerCtx))
                .setLayerObject(
                    LayerObjectSerializer.serializeLayerObject(
                        snapshot.getApiData(serializerCtx, null, null).getLayerData()
                    )
                )
                .build();
        }
    }

    public static StorPoolFreeSpace.Builder buildStorPoolFreeSpace(
        Map.Entry<StorPool, Either<SpaceInfo, ApiRcException>> entry
    )
    {
        StorPool storPool = entry.getKey();

        StorPoolFreeSpace.Builder freeSpaceBuilder = StorPoolFreeSpace.newBuilder()
            .setStorPoolUuid(storPool.getUuid().toString())
            .setStorPoolName(storPool.getName().displayValue);

        entry.getValue().consume(
            spaceInfo -> freeSpaceBuilder
                .setFreeCapacity(spaceInfo.freeCapacity)
                .setTotalCapacity(spaceInfo.totalCapacity),
            apiRcException -> freeSpaceBuilder
                // required field
                .setFreeCapacity(0L)
                // required field
                .setTotalCapacity(0L)
                .addAllErrors(serializeApiCallRc(apiRcException.getApiCallRc()))
        );

        return freeSpaceBuilder;
    }

    public static CryptoEntryOuterClass.CryptoEntry.Builder buildCryptoEntry(ProcCryptoEntry entry)
    {
        return CryptoEntryOuterClass.CryptoEntry.newBuilder()
            .setName(entry.getName())
            .setDriver(entry.getDriver())
            .setPriority(entry.getPriority())
            .setType(entry.getType().getName());
    }
}
