package com.linbit.drbdmanage;

/**
 * Valid name of a drbdmanageNG node
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NodeName extends GenericName
{
    public NodeName(String nodeName) throws InvalidNameException
    {
        super(nodeName);
        Checks.hostNameCheck(nodeName);
    }
}
