package com.linbit.drbdmanage;

import com.linbit.drbdmanage.debug.DebugConsole;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Context information for a peer connected to a drbdmanage Controller or Satellite
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CommonPeerCtx
{
    private AtomicReference<DebugConsole> dbgConsole = new AtomicReference<>();

    public void setDebugConsole(DebugConsole dbgConsoleRef)
    {
        dbgConsole.set(dbgConsoleRef);
    }

    public DebugConsole getDebugConsole()
    {
        return dbgConsole.get();
    }
}
