package com.linbit.linstor.event.common;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.VolumeNumber;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class ResourceState
{
    private final Boolean accessToUpToDateData;
    private final Map<VolumeNumber, Map<Integer, Boolean>> peersConnected;
    private final @Nullable Boolean inUse;
    private final Boolean upToDate;
    private final @Nullable Integer promotionScore;
    private final @Nullable Boolean mayPromote;

    public ResourceState(
        Boolean accessToUpToDateDataRef,
        Map<VolumeNumber, Map<Integer /* peer-node-id */, Boolean /* peer connected */>> peersConnectedRef,
        @Nullable Boolean inUseRef,
        Boolean upToDateRef,
        @Nullable Integer promotionScoreRef,
        @Nullable Boolean mayPromoteRef)
    {
        accessToUpToDateData = accessToUpToDateDataRef;
        peersConnected = Collections.unmodifiableMap(peersConnectedRef);
        inUse = inUseRef;
        upToDate = upToDateRef;
        promotionScore = promotionScoreRef;
        mayPromote = mayPromoteRef;
    }

    public boolean isReady(Collection<Integer> collectionRef)
    {
        return Boolean.TRUE.equals(accessToUpToDateData) && allExpectedPeersOnline(collectionRef, peersConnected);
    }

    public static boolean allExpectedPeersOnline(
        Collection<Integer> expectedOnlineNodeIdsRef,
        Map<VolumeNumber, Map<Integer, Boolean>> peersConnectedRef
    )
    {
        boolean ret = peersConnectedRef != null;
        if (ret)
        {
            for (Map<Integer, Boolean> peerConnectedState : peersConnectedRef.values())
            {
                for (Integer linstorOnlineNodeId : expectedOnlineNodeIdsRef)
                {
                    if (!Boolean.TRUE.equals(peerConnectedState.get(linstorOnlineNodeId)))
                    {
                        ret = false;
                        break;
                    }
                }
            }
        }
        return ret;
    }

    public Boolean hasAccessToUpToDateData()
    {
        return accessToUpToDateData;
    }

    public Map<VolumeNumber, Map<Integer, Boolean>> getPeersConnected()
    {
        return peersConnected;
    }

    public @Nullable Boolean getInUse()
    {
        return inUse;
    }

    public Boolean getUpToDate()
    {
        return upToDate;
    }

    public @Nullable Integer getPromotionScore()
    {
        return promotionScore;
    }

    public @Nullable Boolean mayPromote()
    {
        return mayPromote;
    }
}
