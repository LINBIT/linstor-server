package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.identifier.NodeName;

/**
 * Cause and correction shared by all errors that reject an operation because a resource is still in use.
 */
public final class CtrlRscInUseHelper
{
    private static final String CAUSE = "The resource is Primary, or its DRBD device is still open " +
        "(for example mounted, or opened read-only by a process).";

    private CtrlRscInUseHelper()
    {
    }

    /**
     * Adds the in-use cause and correction to the given entry. Being in use is an expected condition, so no error
     * report is created for it.
     *
     * @param builder the entry of the rejected operation
     * @param nodeName the node on which the resource is in use
     *
     * @return the given builder
     */
    public static ApiCallRcImpl.EntryBuilder addInUseDetails(ApiCallRcImpl.EntryBuilder builder, NodeName nodeName)
    {
        return builder
            .setCause(CAUSE)
            .setCorrection(
                String.format(
                    "Stop everything that uses the device on node '%s' (unmount it, stop processes that hold it " +
                        "open), then retry.",
                    nodeName.displayValue
                )
            )
            .setSkipErrorReport(true);
    }
}
