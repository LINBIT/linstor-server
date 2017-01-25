package com.linbit.drbdmanage;

/**
 * Valid name of a drbdmanageNG node
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NodeName extends GenericName
{
    private NodeName(String nodeName)
    {
        super(nodeName);
    }

    public NodeName fromString(String nodeName)
        throws InvalidNameException
    {
        Checks.hostNameCheck(nodeName);
        return new NodeName(nodeName);
    }
}
