package com.linbit.linstor.debug;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

public class DebugModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        Multibinder<CommonDebugCmd> commandsBinder =
            Multibinder.newSetBinder(binder(), CommonDebugCmd.class);

        commandsBinder.addBinding().to(CmdDisplayThreads.class);
        commandsBinder.addBinding().to(CmdDisplayContextInfo.class);
        commandsBinder.addBinding().to(CmdDisplayServices.class);
        commandsBinder.addBinding().to(CmdDisplayModuleInfo.class);
        commandsBinder.addBinding().to(CmdDisplayVersion.class);
        commandsBinder.addBinding().to(CmdStartService.class);
        commandsBinder.addBinding().to(CmdEndService.class);
        commandsBinder.addBinding().to(CmdDisplayConnections.class);
        commandsBinder.addBinding().to(CmdCloseConnection.class);
        commandsBinder.addBinding().to(CmdDisplaySystemStatus.class);
        commandsBinder.addBinding().to(CmdDisplayApis.class);
        commandsBinder.addBinding().to(CmdDisplayNodes.class);
        commandsBinder.addBinding().to(CmdDisplayStorPoolDfn.class);
        commandsBinder.addBinding().to(CmdDisplayStorPool.class);
        commandsBinder.addBinding().to(CmdDisplayResourceDfn.class);
        commandsBinder.addBinding().to(CmdDisplayResource.class);
        commandsBinder.addBinding().to(CmdDisplayLockStatus.class);
        commandsBinder.addBinding().to(CmdDisplayTraceMode.class);
        commandsBinder.addBinding().to(CmdSetTraceMode.class);
        commandsBinder.addBinding().to(CmdDisplaySecLevel.class);
        commandsBinder.addBinding().to(CmdSetSecLevel.class);
        commandsBinder.addBinding().to(CmdDisplayAuthPolicy.class);
        commandsBinder.addBinding().to(CmdSetAuthPolicy.class);
        commandsBinder.addBinding().to(CmdSetConnectionContext.class);
        commandsBinder.addBinding().to(CmdDisplayReport.class);
        commandsBinder.addBinding().to(CmdDisplayReportList.class);
        commandsBinder.addBinding().to(CmdTestErrorLog.class);
        commandsBinder.addBinding().to(CmdTestProblemLog.class);
        commandsBinder.addBinding().to(CmdShutdown.class);
    }
}
