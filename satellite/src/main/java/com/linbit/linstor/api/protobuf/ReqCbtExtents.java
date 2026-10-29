package com.linbit.linstor.api.protobuf;

import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.drbd.CbtSessionService;
import com.linbit.linstor.layer.drbd.CbtSessionService.CbtExtents;
import com.linbit.linstor.layer.drbd.CbtSessionService.ExtentsMode;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntReqCbtExtentsOuterClass.MsgIntReqCbtExtents;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntCbtExtentsOuterClass.MsgIntCbtExtents;
import com.linbit.linstor.storage.StorageException;
import com.linbit.locks.LockGuardFactory;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

import static com.linbit.linstor.api.protobuf.serializer.ProtoCommonSerializerBuilder.serializeApiCallRc;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_CBT_EXTENTS,
    description = "Returns the changed extents of a CBT session's snapshot volume."
)
@Singleton
public class ReqCbtExtents implements ApiCallReactive
{
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CbtSessionService cbtSessionService;
    private final CommonSerializer commonSerializer;
    private final Provider<Long> apiCallIdProvider;

    @Inject
    public ReqCbtExtents(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CbtSessionService cbtSessionServiceRef,
        CommonSerializer commonSerializerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdProviderRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        cbtSessionService = cbtSessionServiceRef;
        commonSerializer = commonSerializerRef;
        apiCallIdProvider = apiCallIdProviderRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
    {
        // the service takes the locks it needs itself, the dump runs without any
        return scopeRunner.fluxInTransactionlessScope(
            "Query CBT extents",
            lockGuardFactory.createDeferred().build(),
            () -> executeInScope(msgDataIn)
        );
    }

    private Flux<byte[]> executeInScope(InputStream msgDataIn) throws IOException, InvalidNameException
    {
        MsgIntReqCbtExtents reqMsg = MsgIntReqCbtExtents.parseDelimitedFrom(msgDataIn);
        MsgIntCbtExtents.Builder builder = MsgIntCbtExtents.newBuilder();
        try
        {
            CbtExtents extents = cbtSessionService.dump(
                new ResourceName(reqMsg.getRscName()),
                new SnapshotName(reqMsg.getSnapName()),
                reqMsg.getVlmNr(),
                reqMsg.getStartOffset(),
                reqMsg.getMaxExtents(),
                reqMsg.getMode() == MsgIntReqCbtExtents.Mode.ZERO ? ExtentsMode.ZERO : ExtentsMode.DIRTY
            );
            builder.setVolumeSize(extents.volumeSize());
            if (extents.baseCbtId() != null)
            {
                builder.setBaseCbtId(extents.baseCbtId());
            }
            if (extents.since() != null)
            {
                builder.setSince(extents.since());
            }
            for (CbtSessionService.Extent extent : extents.extents())
            {
                builder.addStarts(extent.start());
                builder.addLengths(extent.length());
                if (extent.dirty() != null)
                {
                    builder.addDirty(extent.dirty());
                }
                builder.addZero(extent.zero());
            }
            if (extents.nextOffset() != null)
            {
                builder.setNextOffset(extents.nextOffset());
            }
        }
        catch (StorageException | DatabaseException | RuntimeException exc)
        {
            builder.addAllErrors(serializeApiCallRc(cbtSessionService.toApiCallRc(exc)));
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        builder.build().writeDelimitedTo(baos);
        return Flux.just(
            commonSerializer.answerBuilder(InternalApiConsts.API_REQUEST_CBT_EXTENTS, apiCallIdProvider.get())
                .bytes(baos.toByteArray())
                .build()
        );
    }
}
