package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.api.pojo.SnapshotDfnPojo;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.api.pojo.SnapshotVlmDfnPojo;
import com.linbit.linstor.api.pojo.SnapshotVlmPojo;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.core.apis.SnapshotVolumeApi;
import com.linbit.linstor.core.apis.SnapshotVolumeDefinitionApi;
import com.linbit.linstor.proto.javainternal.c2s.IntSnapshotOuterClass.IntSnapshot;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplySnapshotOuterClass.MsgIntApplySnapshot;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_IN_PROGRESS_SNAPSHOT,
    description = "Applies snapshot update data"
)
@Singleton
public class ApplySnapshot implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplySnapshot(
        StltApiCallHandler apiCallHandlerRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntApplySnapshot msgApplySnapshot = MsgIntApplySnapshot.parseDelimitedFrom(msgDataIn);

        SnapshotPojo snapshotRaw = asSnapshotPojo(
            msgApplySnapshot.getSnapshot(),
            msgApplySnapshot.getFullSyncId(),
            msgApplySnapshot.getUpdateId()
        );
        apiCallHandler.applySnapshotChanges(snapshotRaw);
    }

    static SnapshotPojo asSnapshotPojo(IntSnapshot snapshot, long fullSyncId, long updateId)
    {
        List<SnapshotVolumeDefinitionApi> snapshotVlmDfns =
            snapshot.getSnapshotVlmDfnsList().stream()
                .map(snapshotVlmDfn -> new SnapshotVlmDfnPojo(
                    UUID.fromString(snapshotVlmDfn.getSnapshotVlmDfnUuid()),
                    snapshotVlmDfn.getVlmNr(),
                    snapshotVlmDfn.getVlmSize(),
                    snapshotVlmDfn.getFlags(),
                    snapshotVlmDfn.getSnapshotVlmDfnPropsMap(),
                    snapshotVlmDfn.getVlmDfnPropsMap()
                ))
                .collect(Collectors.toList());

        List<SnapshotVolumeApi> snapshotVlms =
            snapshot.getSnapshotVlmsList().stream()
                .map(snapshotVlm -> new SnapshotVlmPojo(
                    UUID.fromString(snapshotVlm.getSnapshotVlmDfnUuid()),
                    UUID.fromString(snapshotVlm.getSnapshotVlmUuid()),
                    snapshotVlm.getVlmNr(),
                    snapshotVlm.getSnapshotVlmPropsMap(),
                    snapshotVlm.getVlmPropsMap(),
                    snapshotVlm.getState()
                ))
                .collect(Collectors.toList());

        RscLayerDataApi snapLayerData = ProtoLayerUtils.extractRscLayerData(
            snapshot.getLayerObject(),
            fullSyncId,
            updateId
        );
        return new SnapshotPojo(
            new SnapshotDfnPojo(
                new RscDfnPojo(
                    UUID.fromString(snapshot.getRscDfnUuid()),
                    ProtoDeserializationUtils.parseRscGrp(snapshot.getRscGrp()),
                    snapshot.getRscName(),
                    null,
                    snapshot.getRscDfnFlags(),
                    snapshot.getRscDfnPropsMap(),
                    null,
                    Collections.emptyList()
                ),
                UUID.fromString(snapshot.getSnapshotDfnUuid()),
                snapshot.getSnapshotName(),
                snapshotVlmDfns,
                snapshot.getSnapshotDfnFlags(),
                snapshot.getSnapshotDfnPropsMap(),
                snapshot.getSnapshotRscDfnPropsMap(),
                Collections.emptyList(),
                Collections.emptyList()
            ),
            UUID.fromString(snapshot.getSnapshotUuid()),
            snapshot.getFlags(),
            snapshot.getSuspendResource(),
            snapshot.getTakeSnapshot(),
            fullSyncId,
            updateId,
            snapshotVlms,
            snapLayerData,
            snapshot.getNodeName(),
            null,
            snapshot.getSnapshotPropsMap(),
            snapshot.getRscPropsMap()
        );
    }
}
