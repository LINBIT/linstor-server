package com.linbit.linstor;

import java.util.concurrent.atomic.AtomicReference;

import com.linbit.linstor.debug.DebugConsole;

/**
 * Context information for a peer connected to a linstor Controller or Satellite
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
