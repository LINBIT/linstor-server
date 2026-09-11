package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntArchiveLogsOuterClass.MsgIntArchiveLogs;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;


@ProtobufApiCall(
    name = InternalApiConsts.API_ARCHIVE_LOGS,
    description = "Archives old error-report logs files"
)
@Singleton
public class ArchiveLogs implements ApiCall
{
    private final ErrorReporter errorReporter;

    @Inject
    public ArchiveLogs(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
    }


    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        @Nullable MsgIntArchiveLogs msgArchiveLogs = MsgIntArchiveLogs.parseDelimitedFrom(msgDataIn);
        // controllers older than the age_days introduction send this message without any data
        long ageDays = msgArchiveLogs != null ?
            msgArchiveLogs.getAgeDays() :
            ErrorReporter.DFLT_LOG_ARCHIVE_AGE_DAYS;
        errorReporter.archiveLogDirectory(ageDays);
    }
}
