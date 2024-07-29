package com.linbit.linstor;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.debug.DebugConsole;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Context information for a peer connected to a linstor Controller or Satellite
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CommonPeerCtx
{
    private AtomicReference<@Nullable DebugConsole> dbgConsole = new AtomicReference<>();

    public void setDebugConsole(@Nullable DebugConsole dbgConsoleRef)
    {
        dbgConsole.set(dbgConsoleRef);
    }

    public @Nullable DebugConsole getDebugConsole()
    {
        return dbgConsole.get();
    }
}
