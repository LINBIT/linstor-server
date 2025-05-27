package com.linbit.linstor.test.factories;

import com.linbit.linstor.layer.LayerPayload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestFactoryUtils
{
    public static <T> List<T> copyOrNull(List<T> list)
    {
        if (list == null)
        {
            return null;
        }
        return new ArrayList<>(list);
    }

    public static <K, V> Map<K, V> copyOrNull(Map<K, V> map)
    {
        if (map == null)
        {
            return null;
        }
        return new HashMap<>(map);
    }

    public static LayerPayload createCopy(LayerPayload copyFrom)
    {
        LayerPayload ret = new LayerPayload();
        if (copyFrom != null)
        {
            ret.drbdRsc.alStripes = copyFrom.drbdRsc.alStripes;
            ret.drbdRsc.alStripeSize = copyFrom.drbdRsc.alStripeSize;
            ret.drbdRsc.needsNewNodeId = copyFrom.drbdRsc.needsNewNodeId;
            ret.drbdRsc.nodeId = copyFrom.drbdRsc.nodeId;
            ret.drbdRsc.tcpPorts = copyFrom.drbdRsc.tcpPorts;
            ret.drbdRsc.portCount = copyFrom.drbdRsc.portCount;
            ret.drbdRsc.peerSlots = copyFrom.drbdRsc.peerSlots;

            ret.drbdRscDfn.peerSlotsNewResource = copyFrom.drbdRscDfn.peerSlotsNewResource;
            ret.drbdRscDfn.sharedSecret = copyFrom.drbdRscDfn.sharedSecret;
            ret.drbdRscDfn.transportType = copyFrom.drbdRscDfn.transportType;

            ret.drbdVlmDfn.minorNr = copyFrom.drbdVlmDfn.minorNr;

            ret.storagePayload = new HashMap<>(copyFrom.storagePayload);
        }
        return ret;
    }
}
