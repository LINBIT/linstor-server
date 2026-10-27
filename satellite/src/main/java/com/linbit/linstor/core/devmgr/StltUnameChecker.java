package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Warns if the live hostname of this satellite differs from the {@link InternalApiConsts#NODE_UNAME} the controller
 * stored for the local node.
 * <p>
 * The controller only updates {@link InternalApiConsts#NODE_UNAME} when the satellite (re-)authenticates, but the
 * generated .res files use it for the local <code>on</code> section. If the host was renamed in the meantime,
 * <code>drbdadm</code> no longer finds the local host in these files.
 * </p>
 * <p>
 * The warning is logged once per mismatch, not on every device manager run.
 * </p>
 */
@Singleton
public class StltUnameChecker
{
    private final ErrorReporter errorReporter;
    private final Supplier<String> liveUnameSupplier;

    private @Nullable String lastWarnedLiveUname;

    @Inject
    public StltUnameChecker(ErrorReporter errorReporterRef)
    {
        this(errorReporterRef, LinStor::getHostName);
    }

    StltUnameChecker(ErrorReporter errorReporterRef, Supplier<String> liveUnameSupplierRef)
    {
        errorReporter = errorReporterRef;
        liveUnameSupplier = liveUnameSupplierRef;
    }

    /**
     * Compares the local node's stored uname with the live one and logs a warning on a new mismatch.
     *
     * @return true if the stored and the live uname differ
     */
    public synchronized boolean check(Node localNodeRef)
    {
        boolean mismatch = false;
        @Nullable String storedUname = localNodeRef.getProps().getProp(InternalApiConsts.NODE_UNAME);
        String liveUname = liveUnameSupplier.get();
        // an empty live uname means it could not be determined
        if (storedUname != null && liveUname != null && !liveUname.isEmpty())
        {
            mismatch = !storedUname.equals(liveUname);
            if (mismatch)
            {
                if (!Objects.equals(lastWarnedLiveUname, liveUname))
                {
                    errorReporter.logWarning(
                        "The hostname of this satellite changed from '%s' to '%s' since it connected to the " +
                            "controller. The generated DRBD resource files still use '%s', so drbdadm will not find " +
                            "this host in them. Restart the satellite or reconnect the node to update it.",
                        storedUname,
                        liveUname,
                        storedUname
                    );
                    lastWarnedLiveUname = liveUname;
                }
            }
            else if (lastWarnedLiveUname != null)
            {
                errorReporter.logInfo("The hostname of this satellite matches the controller's again: '%s'", liveUname);
                lastWarnedLiveUname = null;
            }
        }
        return mismatch;
    }
}
