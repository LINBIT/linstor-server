package com.linbit.linstor.core;

import java.io.PrintStream;
import java.util.Map;

import com.linbit.ErrorCheck;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.debug.BaseDebugConsole;
import com.linbit.linstor.debug.CommonDebugCmd;
import com.linbit.linstor.debug.SatelliteDebugCmd;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.CommonMessageProcessor;
import com.linbit.linstor.security.AccessContext;

class StltDebugConsoleImpl extends BaseDebugConsole
    {
        private final Satellite satellite;

        private boolean loadedCmds  = false;
        private boolean exitFlag    = false;

        public static final String CONSOLE_PROMPT = "Command ==> ";

        public static final String[] GNRC_COMMAND_CLASS_LIST =
        {
        };
        public static final String GNRC_COMMAND_CLASS_PKG = "com.linbit.linstor.debug";
        public static final String[] STLT_COMMAND_CLASS_LIST =
        {
            "CmdDisplayThreads",
            "CmdDisplayContextInfo",
            "CmdDisplayServices",
            "CmdDisplaySecLevel",
            "CmdDisplayModuleInfo",
            "CmdDisplayVersion",
            "CmdStartService",
            "CmdEndService",
            "CmdDisplayConnections",
            "CmdCloseConnection",
            "CmdDisplaySystemStatus",
            "CmdDisplayApis",
            "CmdDisplayNodes",
            "CmdDisplayStorPoolDfn",
            "CmdDisplayStorPool",
            "CmdDisplayResourceDfn",
            "CmdDisplayResource",
            "CmdDisplayLockStatus",
            "CmdDisplayConfValue",
            "CmdDisplayObjectStatistics",
            "CmdDisplayTraceMode",
            "CmdSetTraceMode",
            "CmdDisplayReport",
            "CmdDisplayReportList",
            "CmdRunDeviceManager",
            "CmdDisplayObjProt",
            "CmdTestErrorLog",
            "CmdShutdown"
        };
        public static final String STLT_COMMAND_CLASS_PKG = "com.linbit.linstor.debug";

        private StltDebugControl debugCtl;

        StltDebugConsoleImpl(
            Satellite satelliteRef,
            AccessContext accCtx,
            Map<ServiceName, SystemService> systemServicesMap,
            Map<String, Peer> peerMap,
            CommonMessageProcessor msgProc
        )
        {
            super(accCtx, satelliteRef);
            ErrorCheck.ctorNotNull(StltDebugConsoleImpl.class, Satellite.class, satelliteRef);
            ErrorCheck.ctorNotNull(StltDebugConsoleImpl.class, AccessContext.class, accCtx);

            satellite = satelliteRef;
            loadedCmds = false;
            debugCtl = new StltDebugControlImpl(
                satellite,
                systemServicesMap,
                peerMap,
                msgProc
            );
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
                for (String cmdClassName : STLT_COMMAND_CLASS_LIST)
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
                    STLT_COMMAND_CLASS_PKG + "." + cmdClassName
                );
                try
                {
                    CommonDebugCmd cmnDebugCmd = (CommonDebugCmd) cmdClass.newInstance();
                    cmnDebugCmd.commonInitialize(satellite, satellite, debugCtl, this);
                    if (cmnDebugCmd instanceof SatelliteDebugCmd)
                    {
                        SatelliteDebugCmd debugCmd = (SatelliteDebugCmd) cmnDebugCmd;
                        debugCmd.initialize(satellite, satellite, debugCtl, this);
                    }

                    // FIXME: Detect and report name collisions
                    for (String cmdName : cmnDebugCmd.getCmdNames())
                    {
                        commandMap.put(cmdName.toUpperCase(), cmnDebugCmd);
                    }
                }
                catch (IllegalAccessException | InstantiationException instantiateExc)
                {
                    satellite.getErrorReporter().reportError(instantiateExc);
                }
            }
            catch (ClassNotFoundException cnfExc)
            {
                satellite.getErrorReporter().reportError(cnfExc);
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