package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlErrorListApiCallHandler;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.proto.requests.MsgReqErrorReportOuterClass.MsgReqErrorReport;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import reactor.core.publisher.Flux;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_REQ_ERROR_REPORTS,
    description = "Returns the requested error reports."
)
@Singleton
public class ReqErrorReports implements ApiCallReactive
{
    private final CtrlErrorListApiCallHandler errorListApiCallHandler;
    private final ScopeRunner scopeRunner;
    private final CtrlClientSerializer clientComSerializer;
    private final Provider<Long> apiCallId;

    @Inject
    public ReqErrorReports(
        CtrlErrorListApiCallHandler apiCallHandlerRef,
        ScopeRunner scopeRunnerRef,
        CtrlClientSerializer clientComSerializerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef
    )
    {
        errorListApiCallHandler = apiCallHandlerRef;
        scopeRunner = scopeRunnerRef;
        clientComSerializer = clientComSerializerRef;
        apiCallId = apiCallIdRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgReqErrorReport reqErrorReport = MsgReqErrorReport.parseDelimitedFrom(msgDataIn);
        Optional<Date> since = Optional.ofNullable(
            reqErrorReport.hasSince() ? new Date(reqErrorReport.getSince()) : null);
        Optional<Date> to = Optional.ofNullable(reqErrorReport.hasTo() ? new Date(reqErrorReport.getTo()) : null);
        Flux<Set<ErrorReport>> reports = errorListApiCallHandler
            .listErrorReports(
                new TreeSet<>(reqErrorReport.getNodeNamesList()),
                reqErrorReport.getWithContent(),
                since,
                to,
                new TreeSet<>(reqErrorReport.getIdsList())
            );

        return reports.flatMap(reportSet ->
            scopeRunner.fluxInTransactionlessScope("Serialize error reports", LockGuard.createDeferred(),
                () -> Flux.just(clientComSerializer
                    .answerBuilder(ApiConsts.API_LST_ERROR_REPORTS, apiCallId.get())
                    .errorReports(reportSet)
                    .build())
            ));
    }
}
