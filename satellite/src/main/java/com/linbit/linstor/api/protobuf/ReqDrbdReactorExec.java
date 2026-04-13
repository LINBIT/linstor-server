package com.linbit.linstor.api.protobuf;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.proto.requests.MsgReqDrbdReactorExecOuterClass.DrbdReactorCommand;
import com.linbit.linstor.proto.requests.MsgReqDrbdReactorExecOuterClass.MsgReqDrbdReactorExec;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Handles requests to execute drbd-reactorctl commands on the satellite node.
 * <p>
 * This API call receives a command execution request from the controller,
 * executes it locally using {@link ExtCmdFactory}, and returns the result
 * including exit code, stdout and stderr.
 * </p>
 * <p>
 * Only specific drbd-reactorctl commands are allowed, defined by the
 * {@link DrbdReactorCommand} proto enum.
 * </p>
 */
@ProtobufApiCall(
    name = InternalApiConsts.API_REQ_DRBD_REACTOR_EXEC,
    description = "Executes a drbd-reactorctl command on the node.",
    transactional = false
)
@Singleton
public class ReqDrbdReactorExec implements ApiCall
{
    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer interComSerializer;
    private final Provider<Long> apiCallId;
    private final ExtCmdFactory extCmdFactory;

    @Inject
    public ReqDrbdReactorExec(
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer interComSerializerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdProviderRef,
        ExtCmdFactory extCmdFactoryRef
    )
    {
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        apiCallId = apiCallIdProviderRef;
        extCmdFactory = extCmdFactoryRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgReqDrbdReactorExec reqExec = MsgReqDrbdReactorExec.parseDelimitedFrom(msgDataIn);

        DrbdReactorCommand command = reqExec.getCommand();
        String config = reqExec.getConfig();

        int exitCode;
        String stdout = "";
        String stderr = "";

        if (command == DrbdReactorCommand.UNKNOWN || command == DrbdReactorCommand.UNRECOGNIZED)
        {
            exitCode = -1;
            stderr = "Unknown or invalid command";
        }
        else
        {
            String[] cmdArgs = getCommandArgs(command, config);
            if (cmdArgs.length == 0)
            {
                exitCode = -1;
                stderr = "Invalid command or missing config parameter for " + command.name();
            }
            else if (command == DrbdReactorCommand.EVICT && !reqExec.getWait())
            {
                // The evict command may stop the LINSTOR controller service as part of the
                // drbd-reactor managed service stack (e.g. linstor_db). In that case the
                // TCP connection from the satellite back to the controller drops while
                // drbd-reactorctl is still blocking (it waits up to 20 s for peer takeover),
                // making it impossible to send the response after the command completes.
                //
                // Default (wait=false): start the evict process asynchronously and respond
                // immediately. The caller polls drbd-reactor status to confirm the result.
                //
                // When wait=true, fall through to the synchronous exec branch below.
                // The caller is responsible for ensuring the eviction will not drop the
                // controller connection (e.g. when evicting a non-LINSTOR service).
                try
                {
                    extCmdFactory.create().asyncExec(cmdArgs);
                    exitCode = 0;
                    stdout = "evict initiated";
                }
                catch (IOException exc)
                {
                    exitCode = -1;
                    stderr = "Failed to start evict: " + exc.getMessage();
                }
            }
            else
            {
                try
                {
                    ExtCmd.OutputData outputData = extCmdFactory.create().exec(cmdArgs);
                    exitCode = outputData.exitCode;
                    stdout = new String(outputData.stdoutData, StandardCharsets.UTF_8);
                    stderr = new String(outputData.stderrData, StandardCharsets.UTF_8);
                }
                catch (Exception exc)
                {
                    exitCode = -1;
                    stderr = exc.getMessage() != null ? exc.getMessage() : exc.toString();
                }
            }
        }

        byte[] build = interComSerializer.answerBuilder(InternalApiConsts.API_RSP_DRBD_REACTOR_EXEC, apiCallId.get())
            .drbdReactorExecResponse(
                exitCode,
                stdout.getBytes(StandardCharsets.UTF_8),
                stderr.getBytes(StandardCharsets.UTF_8)
            )
            .build();

        controllerPeerConnector.getControllerPeer().sendMessage(
            build,
            InternalApiConsts.API_RSP_DRBD_REACTOR_EXEC
        );
    }

    /**
     * Returns the actual command line arguments for the given command.
     */
    @SuppressWarnings("checkstyle:NoWhitespaceAfter")
    private String[] getCommandArgs(DrbdReactorCommand command, String config)
    {
        String[] ret;
        if (command != DrbdReactorCommand.STATUS && (config == null || config.isEmpty()))
        {
            ret = new String[0];
        }
        else
        {
            ret = switch (command)
            {
                case STATUS -> new String[] { "drbd-reactorctl", "status", "--json" };
                case EVICT -> new String[] { "drbd-reactorctl", "evict", config };
                case DISABLE -> new String[] { "drbd-reactorctl", "disable", config };
                case ENABLE -> new String[] { "drbd-reactorctl", "enable", config };
                case RESTART -> new String[] { "drbd-reactorctl", "restart", config };
                case DISABLE_NOW -> new String[] { "drbd-reactorctl", "disable", "--now", config };
                case UNKNOWN, UNRECOGNIZED -> new String[0];
            };
        }
        return ret;
    }
}
