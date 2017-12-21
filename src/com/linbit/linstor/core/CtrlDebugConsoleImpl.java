package com.linbit.linstor.core;

import java.io.PrintStream;
import java.util.Map;

import com.linbit.ErrorCheck;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.ControllerDebugCmd;
import com.linbit.linstor.debug.BaseDebugConsole;
import com.linbit.linstor.debug.CommonDebugCmd;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.CommonMessageProcessor;
import com.linbit.linstor.security.AccessContext;

class CtrlDebugConsoleImpl extends BaseDebugConsole
{
    private final Controller controller;

    public static final String CONSOLE_PROMPT = "Command ==> ";

    private boolean loadedCmds  = false;
    private boolean exitFlag    = false;

    public static final String[] GNRC_COMMAND_CLASS_LIST =
    {
    };
    public static final String GNRC_COMMAND_CLASS_PKG = "com.linbit.linstor.debug";
    public static final String[] CTRL_COMMAND_CLASS_LIST =
    {
        "CmdDisplayThreads",
        "CmdDisplayContextInfo",
        "CmdDisplayServices",
        "CmdDisplaySecLevel",
        "CmdSetSecLevel",
        "CmdDisplayModuleInfo",
        "CmdDisplayVersion",
        "CmdStartService",
        "CmdEndService",
        "CmdDisplayConnections",
        "CmdCloseConnection",
        "CmdDisplaySystemStatus",
        "CmdDisplayApis",
        "CmdDisplayNodes",
        "CmdDisplayResourceDfn",
        "CmdDisplayResource",
        "CmdDisplayLockStatus",
        "CmdDisplayConfValue",
        "CmdSetConfValue",
        "CmdDeleteConfValue",
        "CmdDisplayObjectStatistics",
        "CmdDisplayTraceMode",
        "CmdSetTraceMode",
        "CmdDisplayReport",
        "CmdDisplayReportList",
        "CmdTestErrorLog",
        "CmdShutdown"
    };
    public static final String CTRL_COMMAND_CLASS_PKG = "com.linbit.linstor.debug";

    private CtrlDebugControl debugCtl;

    CtrlDebugConsoleImpl(
        Controller controllerRef,
        AccessContext accCtx,
        Map<ServiceName, SystemService> systemSerivesMap,
        Map<String, Peer> peerMap,
        CommonMessageProcessor msgProc
    )
    {
        super(accCtx, controllerRef);
        ErrorCheck.ctorNotNull(CtrlDebugConsoleImpl.class, Controller.class, controllerRef);
        ErrorCheck.ctorNotNull(CtrlDebugConsoleImpl.class, AccessContext.class, accCtx);

        controller = controllerRef;
        loadedCmds = false;
        debugCtl = new CtrlDebugControlImpl(controller, systemSerivesMap, peerMap, msgProc);
    }

    public void stdStreamsConsole()
    {
        stdStreamsConsole(CONSOLE_PROMPT);
    }

    public void loadDefaultCommands(
        PrintStream debugOut,
        PrintStream debugErr
    )
    {
        if (!loadedCmds)
        {
            for (String cmdClassName : CTRL_COMMAND_CLASS_LIST)
            {
                loadCommand(debugOut, debugErr, cmdClassName);
            }
        }
        loadedCmds = true;
    }

    @Override
    public void loadCommand(
        PrintStream debugOut,
        PrintStream debugErr,
        String cmdClassName
    )
    {
        try
        {
            Class<? extends Object> cmdClass = Class.forName(
                CTRL_COMMAND_CLASS_PKG + "." + cmdClassName
            );
            try
            {
                CommonDebugCmd cmnDebugCmd = (CommonDebugCmd) cmdClass.newInstance();
                cmnDebugCmd.commonInitialize(controller, controller, debugCtl, this);
                if (cmnDebugCmd instanceof ControllerDebugCmd)
                {
                    ControllerDebugCmd debugCmd = (ControllerDebugCmd) cmnDebugCmd;
                    debugCmd.initialize(controller, controller, debugCtl, this);
                }

                // FIXME: Detect and report name collisions
                for (String cmdName : cmnDebugCmd.getCmdNames())
                {
                    commandMap.put(cmdName.toUpperCase(), cmnDebugCmd);
                }
            }
            catch (IllegalAccessException | InstantiationException instantiateExc)
            {
                controller.getErrorReporter().reportError(instantiateExc);
            }
        }
        catch (ClassNotFoundException cnfExc)
        {
            controller.getErrorReporter().reportError(cnfExc);
        }
    }

    @Override
    public void unloadCommand(
        PrintStream debugOut,
        PrintStream debugErr,
        String cmdClassName
    )
    {
        // TODO: Implement
    }
}
