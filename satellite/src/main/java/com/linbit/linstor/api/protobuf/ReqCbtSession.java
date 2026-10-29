package com.linbit.linstor.api.protobuf;

import com.linbit.InvalidNameException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.drbd.CbtSessionService;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntReqCbtSessionOuterClass.MsgIntReqCbtSession;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntCbtSessionOuterClass.MsgIntCbtSession;
import com.linbit.linstor.storage.StorageException;
import com.linbit.locks.LockGuardFactory;

import jakarta.inject.Provider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

import static com.linbit.linstor.api.protobuf.serializer.ProtoCommonSerializerBuilder.serializeApiCallRc;

/**
 * Opens or closes the snapshot of a CBT session. The {@link CbtSessionService} takes the locks it needs itself.
 * Without a {@link ProtobufApiCall} annotation, so that only the subclasses are registered.
 */
abstract class ReqCbtSession implements ApiCallReactive
{
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CbtSessionService cbtSessionService;
    private final CommonSerializer commonSerializer;
    private final Provider<Long> apiCallIdProvider;
    private final String apiCallName;

    ReqCbtSession(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CbtSessionService cbtSessionServiceRef,
        CommonSerializer commonSerializerRef,
        Provider<Long> apiCallIdProviderRef,
        String apiCallNameRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        cbtSessionService = cbtSessionServiceRef;
        commonSerializer = commonSerializerRef;
        apiCallIdProvider = apiCallIdProviderRef;
        apiCallName = apiCallNameRef;
    }

    protected abstract void execute(CbtSessionService serviceRef, ResourceName rscNameRef, SnapshotName snapNameRef)
        throws StorageException, DatabaseException;

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
    {
        return scopeRunner.fluxInTransactionlessScope(
            apiCallName,
            lockGuardFactory.createDeferred().build(),
            () -> executeInScope(msgDataIn)
        );
    }

    private Flux<byte[]> executeInScope(InputStream msgDataIn) throws IOException, InvalidNameException
    {
        MsgIntReqCbtSession reqMsg = MsgIntReqCbtSession.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc;
        try
        {
            execute(
                cbtSessionService,
                new ResourceName(reqMsg.getRscName()),
                new SnapshotName(reqMsg.getSnapName())
            );
            apiCallRc = new ApiCallRcImpl();
        }
        catch (StorageException | DatabaseException | RuntimeException exc)
        {
            apiCallRc = cbtSessionService.toApiCallRc(exc);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MsgIntCbtSession.newBuilder()
            .addAllErrors(serializeApiCallRc(apiCallRc))
            .build()
            .writeDelimitedTo(baos);
        return Flux.just(
            commonSerializer.answerBuilder(apiCallName, apiCallIdProvider.get())
                .bytes(baos.toByteArray())
                .build()
        );
    }
}
