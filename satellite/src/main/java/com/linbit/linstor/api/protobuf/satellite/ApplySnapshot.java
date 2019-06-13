package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.api.pojo.SnapshotDfnPojo;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.api.pojo.SnapshotVlmDfnPojo;
import com.linbit.linstor.api.pojo.SnapshotVlmPojo;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
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

    static SnapshotPojo asSnapshotPojo(IntSnapshot snapshotData, long fullSyncId, long updateId)
    {
        List<SnapshotVolumeDefinition.SnapshotVlmDfnApi> snapshotVlmDfns =
            snapshotData.getSnapshotVlmDfnsList().stream()
                .map(snapshotVlmDfn -> new SnapshotVlmDfnPojo(
                    UUID.fromString(snapshotVlmDfn.getSnapshotVlmDfnUuid()),
                    snapshotVlmDfn.getVlmNr(),
                    snapshotVlmDfn.getVlmSize(),
                    snapshotVlmDfn.getFlags()
                ))
                .collect(Collectors.toList());

        List<SnapshotVolume.SnapshotVlmApi> snapshotVlms =
            snapshotData.getSnapshotVlmsList().stream()
                .map(snapshotVlm -> new SnapshotVlmPojo(
                    snapshotVlm.getStorPoolName(),
                    UUID.fromString(snapshotVlm.getStorPoolUuid()),
                    UUID.fromString(snapshotVlm.getSnapshotVlmDfnUuid()),
                    UUID.fromString(snapshotVlm.getSnapshotVlmUuid()),
                    snapshotVlm.getVlmNr()
                ))
                .collect(Collectors.toList());

        return new SnapshotPojo(
            new SnapshotDfnPojo(
                new RscDfnPojo(
                    UUID.fromString(snapshotData.getRscDfnUuid()),
                    snapshotData.getRscName(),
                    null,
                    snapshotData.getRscDfnFlags(),
                    snapshotData.getRscDfnPropsMap(),
                    null,
                    Collections.emptyList()
                ),
                UUID.fromString(snapshotData.getSnapshotDfnUuid()),
                snapshotData.getSnapshotName(),
                snapshotVlmDfns,
                snapshotData.getSnapshotDfnFlags(),
                snapshotData.getSnapshotDfnPropsMap()
            ),
            UUID.fromString(snapshotData.getSnapshotUuid()),
            snapshotData.getFlags(),
            snapshotData.getSuspendResource(),
            snapshotData.getTakeSnapshot(),
            fullSyncId,
            updateId,
            snapshotVlms
        );
    }
}
