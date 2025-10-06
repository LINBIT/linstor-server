package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.pojo.EbsRemotePojo;
import com.linbit.linstor.api.pojo.ExternalFilePojo;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlStltSerializerBuilder;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandlerUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.storage.utils.ProcCryptoUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.c2s.IntControllerOuterClass.IntController;
import com.linbit.linstor.proto.javainternal.c2s.IntEbsRemoteOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.IntExternalFileOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNode;
import com.linbit.linstor.proto.javainternal.c2s.IntRscOuterClass.IntRsc;
import com.linbit.linstor.proto.javainternal.c2s.IntS3RemoteOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.IntSnapshotOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.IntStorPoolOuterClass.IntStorPool;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyFullSyncOuterClass.MsgIntApplyFullSync;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntFullSyncResponseOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntFullSyncResponseOuterClass.MsgIntFullSyncResponse;
import com.linbit.linstor.storage.ProcCryptoEntry;
import com.linbit.utils.Base64;
import com.linbit.utils.Either;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

@ProtobufApiCall(
    name = InternalApiConsts.API_FULL_SYNC_DATA,
    description = "Transfers initial data for all objects to a satellite"
)
@Singleton
public class FullSync implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final StltApiCallHandlerUtils apiCallHandlerUtils;
    private final ApiCallAnswerer apiCallAnswerer;
    private final ControllerPeerConnector controllerPeerConnector;
    private final Provider<Peer> controllerPeerProvider;
    private final ErrorReporter errorReporter;

    @Inject
    public FullSync(
        StltApiCallHandler apiCallHandlerRef,
        StltApiCallHandlerUtils apiCallHandlerUtilsRef,
        ApiCallAnswerer apiCallAnswererRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        Provider<Peer> controllerPeerProviderRef,
        ErrorReporter errorReporterRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallHandlerUtils = apiCallHandlerUtilsRef;
        apiCallAnswerer = apiCallAnswererRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        controllerPeerProvider = controllerPeerProviderRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntApplyFullSync applyFullSync = MsgIntApplyFullSync.parseDelimitedFrom(msgDataIn);
        long fullSyncId = applyFullSync.getFullSyncTimestamp();
        long updateId = 0;

        IntController msgIntControllerData = applyFullSync.getCtrl();
        Set<NodePojo> nodes = new TreeSet<>(asNodes(applyFullSync.getNodesList(), fullSyncId, updateId));
        Set<StorPoolPojo> storPools = new TreeSet<>(asStorPool(applyFullSync.getStorPoolsList(), fullSyncId, updateId));
        Set<RscPojo> resources = new TreeSet<>(asResources(applyFullSync.getRscsList(), fullSyncId, updateId));
        Set<SnapshotPojo> snapshots = new TreeSet<>(
            asSnapshots(
                applyFullSync.getSnapshotsList(),
                fullSyncId,
                updateId
            )
        );
        Set<ExternalFilePojo> extFiles = new TreeSet<>(
            asExternalFiles(
                applyFullSync.getExternalFilesList(),
                fullSyncId,
                updateId
            )
        );
        Set<S3RemotePojo> s3remotes = new TreeSet<>(asS3Remote(applyFullSync.getS3RemotesList(), fullSyncId, updateId));
        Set<EbsRemotePojo> ebsRemotes = new TreeSet<>(
            asEbsRemote(applyFullSync.getEbsRemotesList(), fullSyncId, updateId)
        );

        FullSyncResult fullSyncResult = apiCallHandler.applyFullSync(
            msgIntControllerData.getPropsMap(),
            nodes,
            storPools,
            resources,
            snapshots,
            extFiles,
            s3remotes,
            ebsRemotes,
            applyFullSync.getFullSyncTimestamp(),
            Base64.decode(applyFullSync.getMasterKey()),
            applyFullSync.getCryptHash().toByteArray(),
            applyFullSync.getCryptSalt().toByteArray(),
            applyFullSync.getEncCryptKey().toByteArray()
        );

        MsgIntFullSyncResponse.Builder builder = MsgIntFullSyncResponse.newBuilder()
            .putAllNodePropsToSet(fullSyncResult.stltPropsToAdd)
            .addAllNodePropKeysToDelete(fullSyncResult.stltPropKeysToDelete)
            .addAllNodePropNamespacesToDelete(fullSyncResult.stltPropNamespacesToDelete);

        switch (fullSyncResult.status)
        {
            case FAIL_MISSING_REQUIRED_EXT_TOOLS:
                errorReporter.logError("FullSync error: missing required ext tools %d", fullSyncId);
                builder.setFullSyncResult(
                    MsgIntFullSyncResponseOuterClass.FullSyncResult.FAIL_MISSING_REQUIRED_EXT_TOOLS
                );
                break;
            case FAIL_UNKNOWN:
                errorReporter.logError("FullSync error: unknown %d", fullSyncId);
                builder.setFullSyncResult(MsgIntFullSyncResponseOuterClass.FullSyncResult.FAIL_UNKNOWN);
                break;
            case SUCCESS:
                errorReporter.logInfo("FullSync successful %d", fullSyncId);
                builder.setFullSyncResult(MsgIntFullSyncResponseOuterClass.FullSyncResult.SUCCESS);
                break;
            default:
                errorReporter.logError("FullSync error: unknown case %s/%d", fullSyncResult, fullSyncId);
                builder.setFullSyncResult(MsgIntFullSyncResponseOuterClass.FullSyncResult.FAIL_UNKNOWN);
                break;
        }
        if (fullSyncResult.status == FullSyncStatus.SUCCESS)
        {
            List<ProcCryptoEntry> cryptoEntries = ProcCryptoUtils.parseProcCrypto(errorReporter);
            for (ProcCryptoEntry procCryptoEntry : cryptoEntries)
            {
                builder.addCryptoEntries(ProtoCtrlStltSerializerBuilder.buildCryptoEntry(procCryptoEntry));
            }

            Map<StorPoolInfo, Either<SpaceInfo, ApiRcException>> spaceInfoQueryMap =
                apiCallHandlerUtils.getAllSpaceInfo();

            for (Entry<StorPoolInfo, Either<SpaceInfo, ApiRcException>> entry : spaceInfoQueryMap.entrySet())
            {
                builder.addFreeSpace(ProtoCtrlStltSerializerBuilder.buildStorPoolFreeSpace(entry).build());
            }
        }
        errorReporter.logInfo("FullSync sending response %d", fullSyncId);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        builder.build().writeDelimitedTo(baos);
        controllerPeerProvider.get().sendMessage(
            apiCallAnswerer.answerBytes(
                baos.toByteArray(),
                InternalApiConsts.API_FULL_SYNC_RESPONSE
            ),
            InternalApiConsts.API_FULL_SYNC_RESPONSE
        );
        System.gc();
    }

    private ArrayList<NodePojo> asNodes(
        List<IntNode> nodesList,
        long fullSyncId,
        long updateId
    )
    {
        ArrayList<NodePojo> nodes = new ArrayList<>(nodesList.size());

        for (IntNode node : nodesList)
        {
            nodes.add(ApplyNode.asNodePojo(node, fullSyncId, updateId));
        }
        return nodes;
    }

    private ArrayList<StorPoolPojo> asStorPool(
        List<IntStorPool> storPoolsList,
        long fullSyncId,
        long updateId
    )
    {
        ArrayList<StorPoolPojo> storPools = new ArrayList<>(storPoolsList.size());
        for (IntStorPool storPool : storPoolsList)
        {
            storPools.add(ApplyStorPool.asStorPoolPojo(storPool, fullSyncId, updateId));
        }
        return storPools;
    }

    private ArrayList<RscPojo> asResources(
        List<IntRsc> rscsList,
        long fullSyncId,
        long updateId
    )
    {
        ArrayList<RscPojo> rscs = new ArrayList<>(rscsList.size());
        for (IntRsc rscData : rscsList)
        {
            rscs.add(ApplyRsc.asRscPojo(rscData, fullSyncId, updateId));
        }
        return rscs;
    }

    private ArrayList<SnapshotPojo> asSnapshots(
        List<IntSnapshotOuterClass.IntSnapshot> snapshotsList,
        long fullSyncId,
        long updateId
    )
    {
        ArrayList<SnapshotPojo> snapshots = new ArrayList<>(snapshotsList.size());
        for (IntSnapshotOuterClass.IntSnapshot snapshot : snapshotsList)
        {
            snapshots.add(ApplySnapshot.asSnapshotPojo(snapshot, fullSyncId, updateId));
        }
        return snapshots;
    }

    private ArrayList<ExternalFilePojo> asExternalFiles(
        List<IntExternalFileOuterClass.IntExternalFile> externalFilesList,
        long fullSyncId,
        long updateId
    )
    {
        ArrayList<ExternalFilePojo> ret = new ArrayList<>(externalFilesList.size());
        for (IntExternalFileOuterClass.IntExternalFile externalFile : externalFilesList)
        {
            ret.add(ApplyExternalFile.asExternalFilePojo(externalFile, fullSyncId, updateId));
        }
        return ret;
    }

    private ArrayList<S3RemotePojo> asS3Remote(
        List<IntS3RemoteOuterClass.IntS3Remote> s3remoteList,
        long fullSyncId,
        long updateId
    )
    {
        ArrayList<S3RemotePojo> ret = new ArrayList<>(s3remoteList.size());
        for (IntS3RemoteOuterClass.IntS3Remote s3remote : s3remoteList)
        {
            ret.add(ApplyRemote.asS3RemotePojo(s3remote, fullSyncId, updateId));
        }
        return ret;
    }

    private ArrayList<EbsRemotePojo> asEbsRemote(
        List<IntEbsRemoteOuterClass.IntEbsRemote> ebsRemoteList,
        long fullSyncId,
        long updateId
    )
    {
        ArrayList<EbsRemotePojo> ret = new ArrayList<>(ebsRemoteList.size());
        for (IntEbsRemoteOuterClass.IntEbsRemote s3remote : ebsRemoteList)
        {
            ret.add(ApplyRemote.asEbsRemotePojo(s3remote, fullSyncId, updateId));
        }
        return ret;
    }

    public enum FullSyncStatus
    {
        SUCCESS,
        FAIL_MISSING_REQUIRED_EXT_TOOLS,
        FAIL_UNKNOWN;
    }

    public static class FullSyncResult
    {
        private final FullSyncStatus status;
        private final Map<String, String> stltPropsToAdd;
        private final Set<String> stltPropKeysToDelete;
        private final Set<String> stltPropNamespacesToDelete;

        public FullSyncResult(
            FullSyncStatus statusRef,
            Map<String, String> stltPropsToAddRef,
            Set<String> stltPropKeysToDeleteRef,
            Set<String> stltPropNamespacesToDeleteRef
        )
        {
            status = statusRef;
            stltPropsToAdd = stltPropsToAddRef;
            stltPropKeysToDelete = stltPropKeysToDeleteRef;
            stltPropNamespacesToDelete = stltPropNamespacesToDeleteRef;
        }

        public static FullSyncResult createFailedResult(FullSyncStatus statusRef)
        {
            return new FullSyncResult(
                statusRef,
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet()
            );
        }
    }
}
