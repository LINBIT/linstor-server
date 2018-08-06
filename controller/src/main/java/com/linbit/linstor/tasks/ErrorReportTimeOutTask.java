package com.linbit.linstor.tasks;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.locks.LockGuard;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

public class ErrorReportTimeOutTask implements TaskScheduleService.Task
{

    private final int TIMEOUT_SECS = 3;

    private ErrorReporter errorReporter;
    private final CommonSerializer commonSerializer;
    private final ReadWriteLock ctrlErrorListLock;

    @Inject
    ErrorReportTimeOutTask(
        ErrorReporter errorReporterRef,
        CommonSerializer commonSerializerRef,
        @Named(ControllerCoreModule.CTRL_ERROR_LIST_LOCK) ReadWriteLock errorListLockRef
    )
    {
        errorReporter = errorReporterRef;
        commonSerializer = commonSerializerRef;
        ctrlErrorListLock = errorListLockRef;
    }

    @Override
    public long run()
    {
        try (LockGuard ls = LockGuard.createLocked(ctrlErrorListLock.writeLock()))
        {
            List<Pair<Peer, Long>> deleteList = new ArrayList<>();
            for (Map.Entry<Pair<Peer, Long>, CtrlNodeApiCallHandler.ErrorReportRequest> entry :
                 CtrlNodeApiCallHandler.errorReportMap.entrySet())
            {
                CtrlNodeApiCallHandler.ErrorReportRequest errorReportRequest = entry.getValue();

                // If request timed out, send error apicallrc
                if (errorReportRequest.requestTime.plusSeconds(TIMEOUT_SECS).isBefore(LocalDateTime.now()) &&
                    !errorReportRequest.requestNodes.isEmpty())
                {
                    ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                    apiCallRc.addEntry(
                        "No error reports received from: " +
                        errorReportRequest.requestNodes, ApiConsts.MASK_ERROR
                    );
                    ApiCallAnswerer apiCallAnswerer = new ApiCallAnswerer(
                        errorReporter,
                        commonSerializer,
                        entry.getKey().objA,
                        entry.getKey().objB
                    );
                    apiCallAnswerer.answerApiCallRc(apiCallRc);
                    deleteList.add(entry.getKey());
                }
            }

            // Remove timed out requests
            deleteList.forEach(peer -> CtrlNodeApiCallHandler.errorReportMap.remove(peer));
        }
        return 1000;
    }
}
