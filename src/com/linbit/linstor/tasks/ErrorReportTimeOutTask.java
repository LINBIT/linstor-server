package com.linbit.linstor.tasks;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.CtrlNodeApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.utils.LockSupport;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

public class ErrorReportTimeOutTask implements TaskScheduleService.Task {

    private final int TIMEOUT_SECS = 3;

    private ErrorReporter errorReporter;
    private final ReadWriteLock ctrlErrorListLock;

    @Inject
    ErrorReportTimeOutTask(
        ErrorReporter errorReporterRef,
        @Named(ControllerCoreModule.CTRL_ERROR_LIST_LOCK) ReadWriteLock errorListLockRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlErrorListLock = errorListLockRef;
    }

    @Override
    public long run() {
        try (LockSupport ls = LockSupport.lock(ctrlErrorListLock.writeLock())) {
            List<Pair<Peer, Integer>> deleteList = new ArrayList<>();
            for (Map.Entry<Pair<Peer, Integer>, CtrlNodeApiCallHandler.ErrorReportRequest> entry :
                CtrlNodeApiCallHandler.errorReportMap.entrySet()) {
                CtrlNodeApiCallHandler.ErrorReportRequest errorReportRequest = entry.getValue();

                // if request timed out, send error apicallrc
                if (errorReportRequest.requestTime.plusSeconds(TIMEOUT_SECS).isBefore(LocalDateTime.now()) &&
                    !errorReportRequest.requestNodes.isEmpty()) {

                    ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                    apiCallRc.addEntry("No error reports received from: " + errorReportRequest.requestNodes, ApiConsts.MASK_ERROR);
                    ApiCallAnswerer apiCallAnswerer = new ApiCallAnswerer(
                        errorReporter,
                        entry.getKey().objA,
                        entry.getKey().objB
                    );
                    apiCallAnswerer.answerApiCallRc(apiCallRc);
                    deleteList.add(entry.getKey());
                }
            }

            // remove timed out requests
            deleteList.forEach(peer -> CtrlNodeApiCallHandler.errorReportMap.remove(peer));
        }
        return 1000;
    }
}
