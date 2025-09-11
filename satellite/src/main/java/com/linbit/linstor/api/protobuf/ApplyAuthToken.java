package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyAuthTokenOuterClass.MsgIntApplyAuthToken;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_AUTH_TOKEN,
    description = "Applies an auth token received from the controller"
)
@Singleton
public class ApplyAuthToken implements ApiCall
{
    private static final Path AUTH_JSON_PATH = Paths.get(LinStor.CONFIG_PATH, "auth.json");

    private final ErrorReporter errorReporter;

    @Inject
    public ApplyAuthToken(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntApplyAuthToken msg = MsgIntApplyAuthToken.parseDelimitedFrom(msgDataIn);
        String authToken = msg.getAuthToken();

        String json = "{\"token\": \"" + authToken + "\"}\n";
        Files.write(AUTH_JSON_PATH, json.getBytes(StandardCharsets.UTF_8));

        errorReporter.logInfo("Auth token written to %s", AUTH_JSON_PATH);
    }
}
