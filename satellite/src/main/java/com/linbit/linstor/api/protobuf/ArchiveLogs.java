package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;

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
        errorReporter.archiveLogDirectory();
    }
}
