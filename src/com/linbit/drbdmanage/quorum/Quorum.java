package com.linbit.drbdmanage.quorum;

import java.util.HashSet;
import java.util.Set;

import com.linbit.Checks;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.Node.NodeFlag;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.stateflags.StateFlags;
import java.sql.SQLException;
import java.util.TreeSet;

/**
 * Implements a simple quorum algorithm
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class Quorum
{
    public static final int COUNT_MIN = 1;
    public static final int COUNT_MAX = 31;

    public static final int FULL_MIN = 3;

    private final Controller controller;
    private final CoreServices coreSvcs;
    private final Set<Node> quorumNodes = new HashSet<>();

    private int quorumCount = 1;
    private int quorumFull = 1;

    public Quorum(Controller controllerRef, CoreServices coreSvcsRef)
    {
        this.controller = controllerRef;
        this.coreSvcs = coreSvcsRef;
    }

    /**
     * Returns true if a change has to be performed (e.g. clear the FLAG_QIGNORE from the new node)
     *
     * @param node The node to join the quorum
     * @return
     * @throws AccessDeniedException
     *
     * TODO: Clearing the node's QIGNORE flag might be possible in this method instead of
     *       returning a change request to the caller
     */
    public boolean nodeJoined(Node node, AccessContext accCtx) throws AccessDeniedException
    {
        boolean changeFlag = false;
        if (!quorumNodes.contains(node))
        {
            if (node != null)
            {
                StateFlags<NodeFlag> nodeFlags = node.getFlags();
                if (node.hasNodeType(accCtx, Node.NodeType.CONTROLLER))
                {
                    if (nodeFlags.isSet(accCtx, NodeFlag.QIGNORE))
                    {
                        changeFlag = true;
                    }
                    quorumCount ++;

                    if (quorumCount < COUNT_MAX)
                    {
                        if (quorumCount > quorumFull)
                        {
                            quorumFull = quorumCount;
                            // TODO: log that quorumFull changed
                        }
                        // TODO: log that the node X joined the partition
                    }
                    else
                    {
                        // TODO: log that the node could not be added (COUNT_MAX reached)
                    }
                }
                else
                {
                    // TODO: log that diskless node joined the partition (count unchanged)
                }
            }
            else
            {
                throw new NullPointerException("Cannot add null to Quorum");
            }
        }
        return changeFlag;
    }

    /**
     * Removes a node from the quorum
     *
     * @param node
     */
    public void nodeLeft(Node node)
    {
        if (quorumNodes.remove(node))
        {
            quorumCount--;
            // TODO: log that the node has been removed
        }
        else
        {
            // TODO: log that the node was not in the partition - qourumCount unchanged
        }
    }

    /**
     * Indicates whether the partition has a quorum or not
     * @return True if the partition has a quorum
     */
    public boolean isPresent()
    {
        return quorumFull < FULL_MIN || quorumCount >= ((quorumFull / 2) + 1);
    }

    /**
     * Returns a clone of the set of nodes of the quorum
     */
    public Set<Node> getQuorumNodes()
    {
        return new TreeSet<>(quorumNodes);
    }

    public boolean isActiveMemberNode(Node node)
    {
        return quorumNodes.contains(node);
    }

    /**
     * Sets the quorumFull if the count is between 1 and COUNT_MAX.
     * Throws {@link IllegalArgumentException} otherwise.
     * @throws ValueOutOfRangeException
     */
    public void setFullMemberCount(int count) throws ValueOutOfRangeException
    {
        Checks.rangeCheck(count, 1, COUNT_MAX);
        if (count > quorumCount)
        {
            quorumFull = count;
            // TODO: log that the expected number of nodes changed
        }
    }

    /**
     * @return The maximum number of nodes that are expected as quorum members
     */
    public int getFullMemberCount()
    {
        return quorumFull;
    }

    /**
     * @return The number of currently active member nodes
     */
    public int getActiveMemberCount()
    {
        return quorumCount;
    }

    /**
     * Readjusts the maximum number of nodes that are expected as quorum members
     * @throws AccessDeniedException
     */
    public void readjustFullMemberCount(AccessContext accCtx) throws AccessDeniedException
    {
        if (true) // workaround so that the compiler just "warns" about dead code instead of reporting an error :)
        {
            // throw an exception as this method cannot work in its current state.
            throw new UnsupportedOperationException();
        }
        int fullCount = 1;

        Set<Node> nodes = null; // FIXME: list of all control nodes
        Node controlNode = null; // FIXME: the node that this Quorum instance is running on

        for (Node node : nodes)
        {
            StateFlags<NodeFlag> nodeFlags = node.getFlags();
            if (node != controlNode &&
                node.hasNodeType(accCtx, Node.NodeType.CONTROLLER) &&
                !nodeFlags.isSet(accCtx, NodeFlag.QIGNORE))
            {
                fullCount++;
            }
        }
        int prevFull = quorumFull;
        if (fullCount <= COUNT_MAX)
        {
            if (fullCount >= quorumFull)
            {
                quorumFull = fullCount;
            }
        }
        else
        {
            quorumFull = COUNT_MAX;
        }
        if (prevFull != quorumFull)
        {
            // TODO: log that the expected number of nodes changed from .. to ..
        }
    }

    /**
     * Clears {@link NodeFlag#QIGNORE} on each connected node
     * @param accCtx
     * @throws AccessDeniedException
     */
    public void readjustQignoreFlags(AccessContext accCtx, TransactionMgr transMgr)
        throws AccessDeniedException, SQLException
    {
        for (Node node : quorumNodes)
        {
            StateFlags<NodeFlag> flags = node.getFlags();
            flags.setConnection(transMgr);
            flags.disableFlags(accCtx, NodeFlag.QIGNORE);
        }
    }
}
