package com.linbit.linstor.event.common;

import com.linbit.linstor.core.identifier.VolumeNumber;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class ResourceState
{
    private final Boolean accessToUpToDateData;
    private final Map<VolumeNumber, Map<Integer, Boolean>> peersConnected;
    private final Boolean inUse;
    private final Boolean upToDate;
    private final Integer promotionScore;
    private final Boolean mayPromote;

    public ResourceState(
        Boolean accessToUpToDateDataRef,
        Map<VolumeNumber, Map<Integer /* peer-node-id */, Boolean /* peer connected */>> peersConnectedRef,
        Boolean inUseRef,
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
        boolean connectedToAllOnlineLinstorNodes = true;
        for (Map<Integer, Boolean> peerConnectedState : peersConnected.values())
        {
            for (Integer linstorOnlineNodeId : collectionRef)
            {
                if (!Boolean.TRUE.equals(peerConnectedState.get(linstorOnlineNodeId)))
                {
                    connectedToAllOnlineLinstorNodes = false;
                    break;
                }
            }
        }
        return Boolean.TRUE.equals(accessToUpToDateData) && connectedToAllOnlineLinstorNodes;
    }

    public Boolean hasAccessToUpToDateData()
    {
        return accessToUpToDateData;
    }

    public Map<VolumeNumber, Map<Integer, Boolean>> getPeersConnected()
    {
        return peersConnected;
    }

    public Boolean getInUse()
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
