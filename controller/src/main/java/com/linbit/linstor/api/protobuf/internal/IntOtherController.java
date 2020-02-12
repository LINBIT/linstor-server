package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_OTHER_CONTROLLER,
    description = "Satellite received a connection from a newer controller. Close connection to it"
)
@Singleton
public class IntOtherController implements ApiCall
{
    private final Provider<Peer> peerProvider;
    private final ErrorReporter errorReporter;

    @Inject
    public IntOtherController(
        Provider<Peer> peerProvideRef,
        ErrorReporter errerReporterRef
    )
    {
        peerProvider = peerProvideRef;
        errorReporter = errerReporterRef;
    }

    @Override
    public void execute(InputStream msgDataInRef) throws IOException
    {
        Peer satellite = peerProvider.get();
        satellite.setAuthenticated(false);
        satellite.setConnectionStatus(ApiConsts.ConnectionStatus.OTHER_CONTROLLER);
        satellite.closeConnection(false);
        errorReporter.logWarning(
            "Satellite " + satellite.getNode().getName() + " has established a connection to a different controller"
        );
    }
}
