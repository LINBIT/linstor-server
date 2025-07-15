package com.linbit.linstor.core.apicallhandler.controller.req;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDescription;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

public class CreateMultiSnapRequest
{
    private static final AtomicLong ID_GEN = new AtomicLong();
    /**
     * Since descr and joinedRscNames might change (i.e. when updated since a few SnapshotDefinitions might have gotten
     * deleted), it might be hard to follow the logs when the description changes. Therefore, the description also
     * includes this id to make it easier to trace. JoinedRscNames however will not include this id since that string is
     * supposed to go towards the client, not the logs
     */
    private final long id = ID_GEN.incrementAndGet();
    /**
     * Input parameter from the user. Might be empty if the request does not come from the user but from the controller
     * (via i.e. a background task like autoSnapshot or scheduled backup)
     */
    private final Collection<SnapReq> snapshots;
    /**
     * The description string which can be used towards a client or logging
     */
    private @Nullable String descr;
    /**
     * Only the resource names in format [rsc1, rsc2]. Also mostly useful towards the client or logging
     */
    private @Nullable String joinedRscNames;

    /**
     * created snapshot definitions. Be careful, the SnapshotDefinitions in this collections can already have been
     * deleted, so it is advised to always perform snapDfn.isDeleted() checks when iterating through this collection.
     */
    private @Nullable Collection<SnapshotDefinition> createdSnapDfns = null;

    public CreateMultiSnapRequest(Collection<SnapReq> snapshotsRef)
    {
        snapshots = snapshotsRef;
        try
        {
            updateDescriptionAndJoinedRscNames(null);
        }
        catch (AccessDeniedException exc)
        {
            // should never happen since createSnapDfns is not yet initialized
            throw new ImplementationError(exc);
        }
    }

    public CreateMultiSnapRequest(AccessContext accCtx, SnapshotDefinition snapDfnRef) throws AccessDeniedException
    {
        snapshots = Collections.singleton(new SnapReq(accCtx, snapDfnRef));
        setCreatedSnapDfns(Collections.singletonList(snapDfnRef));
        updateDescriptionAndJoinedRscNames(accCtx);
    }

    /**
     * Updates the collection of createdSnapshotDefinitions (filters away deleted entries) as well as regenerates the
     * description and the joinedRscNames
     */
    public void update(AccessContext accCtx) throws AccessDeniedException
    {
        updateCreatedSnapDfns();
        updateDescriptionAndJoinedRscNames(accCtx);
    }

    /**
     * This method (re-) initializes the description as well as
     *
     * @param accCtx
     *
     * @throws AccessDeniedException
     */
    public void updateDescriptionAndJoinedRscNames(@Nullable AccessContext accCtx) throws AccessDeniedException
    {
        StringBuilder descrBuilder = new StringBuilder("MultiSnapshot ").append(id).append(" [");
        StringBuilder joinedRscNamesBuilder = new StringBuilder("[");
        List<String> descriptions = new ArrayList<>();
        List<String> resourceNames = new ArrayList<>();

        if (createdSnapDfns == null)
        {
            for (SnapReq snap : snapshots)
            {
                descriptions.add(snap.getDescription());
                resourceNames.add(snap.getRscName());
            }
        }
        else
        {
            for (SnapshotDefinition snapDfn : createdSnapDfns)
            {
                if (!snapDfn.isDeleted())
                {
                    Set<String> nodeNames = new TreeSet<>();
                    for (Snapshot snap : snapDfn.getAllSnapshots(accCtx))
                    {
                        nodeNames.add(snap.getNodeName().displayValue);
                    }
                    String snapDfnDescr = CtrlSnapshotApiCallHandler.getSnapshotDescription(
                        nodeNames,
                        snapDfn.getResourceName().displayValue,
                        snapDfn.getName().displayValue
                    );

                    descriptions.add(snapDfnDescr);
                    resourceNames.add(snapDfn.getResourceName().getDisplayName());
                }
            }
        }

        descrBuilder.append(String.join(", ", descriptions));
        descrBuilder.append("]");
        joinedRscNamesBuilder.append(String.join(", ", resourceNames));
        joinedRscNamesBuilder.append("]");

        descr = descrBuilder.toString();
        joinedRscNames = joinedRscNamesBuilder.toString();
    }

    /**
     * This method removes all already deleted SnapshotDefinitions (snapDfn.isDeleted() == true) from its internal
     * collection that can be accessed via {@link #getCreatedSnapDfns()}
     */
    public void updateCreatedSnapDfns()
    {
        if (createdSnapDfns != null)
        {
            ArrayList<SnapshotDefinition> snapDfnToKeep = new ArrayList<>();
            for (SnapshotDefinition snapDfn : createdSnapDfns)
            {
                if (!snapDfn.isDeleted())
                {
                    snapDfnToKeep.add(snapDfn);
                }
            }
            // prevent possible ConcurrentModificationException
            createdSnapDfns = snapDfnToKeep;
        }
    }

    public ResponseContext makeSnapshotContext()
    {
        ResponseContext ret;
        if (snapshots.size() == 1)
        {
            ret = snapshots.iterator().next().makeContext();
        }
        else
        {
            final Map<String, String> objRefs = new TreeMap<>();

            ret = new ResponseContext(
                ApiOperation.makeCreateOperation(),
                descr,
                descr,
                ApiConsts.MASK_SNAPSHOT,
                objRefs
            );
        }
        return ret;
    }

    public void setCreatedSnapDfns(List<SnapshotDefinition> snapDfnListRef)
    {
        // copy the list not only to prevent side effects when the original list is modified, but also that we can
        // update our createdSnapDfns in the updateCreatedSnapDfns() method
        createdSnapDfns = new ArrayList<>(snapDfnListRef);
    }

    public @Nullable Collection<SnapshotDefinition> getCreatedSnapDfns()
    {
        return createdSnapDfns;
    }

    public Collection<SnapReq> getSnapRequests()
    {
        return snapshots;
    }

    public @Nullable String getDescription()
    {
        return descr;
    }

    public @Nullable String getJoinedRscNames()
    {
        return joinedRscNames;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(snapshots);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof CreateMultiSnapRequest))
        {
            return false;
        }
        CreateMultiSnapRequest other = (CreateMultiSnapRequest) obj;
        return Objects.equals(snapshots, other.snapshots);
    }

    /**
     * This class usually holds the input variables from the user, or in some cases the
     * raw data created by some background tasks. These data should be converted in the
     * very first step into the createdSnapshotDefinitions collections in {@link CreateMultiSnapRequest}
     */
    public static class SnapReq
    {
        private final List<String> nodeNames;
        private final String rscName;
        private final String snapName;
        private final Map<String, String> props;

        public SnapReq(List<String> nodeNamesRef, String rscNameRef, String snapNameRef, Map<String, String> propsRef)
        {
            nodeNames = nodeNamesRef;
            rscName = rscNameRef;
            snapName = snapNameRef;
            props = propsRef;
        }

        public SnapReq(AccessContext accCtx, SnapshotDefinition snapDfnRef) throws AccessDeniedException
        {
            nodeNames = new ArrayList<>();
            props = Collections.emptyMap();
            if (!snapDfnRef.isDeleted())
            {
                for (Snapshot snap : snapDfnRef.getAllSnapshots(accCtx))
                {
                    if (!snap.isDeleted())
                    {
                        nodeNames.add(snap.getNodeName().displayValue);
                    }
                }
                rscName = snapDfnRef.getResourceName().displayValue;
                snapName = snapDfnRef.getName().displayValue;
            }
            else
            {
                rscName = "<deleted>";
                snapName = "<deleted>";
            }
        }

        public ResponseContext makeContext()
        {
            return CtrlSnapshotApiCallHandler.makeSnapshotContext(
                ApiOperation.makeCreateOperation(),
                nodeNames,
                rscName,
                snapName
            );
        }

        public List<String> getNodeNames()
        {
            return nodeNames;
        }

        public String getRscName()
        {
            return rscName;
        }

        public String getSnapName()
        {
            return snapName;
        }

        public String getDescription()
        {
            return getSnapshotDescription(nodeNames, rscName, snapName);
        }

        public Map<String, String> getProps()
        {
            return props;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodeNames, rscName, snapName);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof SnapReq))
            {
                return false;
            }
            SnapReq other = (SnapReq) obj;
            return Objects.equals(nodeNames, other.nodeNames) && Objects.equals(rscName, other.rscName) && Objects
                .equals(snapName, other.snapName);
        }
    }
}
