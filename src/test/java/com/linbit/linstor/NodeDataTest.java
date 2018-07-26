package com.linbit.linstor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.linbit.linstor.api.ApiConsts;

import java.util.List;

import org.junit.Test;

/**
 *
 * @author rp
 */
public class NodeDataTest
{
    @Test
    public void testAllFlags()
    {
        final long mask = Node.NodeFlag.DELETE.flagValue |
                Node.NodeFlag.QIGNORE.flagValue;
        List<String> strList = Node.NodeFlag.toStringList(mask);
        assertEquals(Node.NodeFlag.values().length, strList.size());

        assertArrayEquals(
                new String[]{ApiConsts.FLAG_DELETE, ApiConsts.FLAG_QIGNORE},
                strList.toArray());
        assertEquals(mask, Node.NodeFlag.fromStringList(strList));
    }

    @Test
    public void testFlags()
    {
        {
            final long mask = Node.NodeFlag.DELETE.flagValue |
                    Node.NodeFlag.QIGNORE.flagValue;
            List<String> strList = Node.NodeFlag.toStringList(mask);

            assertArrayEquals(
                    new String[]{ApiConsts.FLAG_DELETE, ApiConsts.FLAG_QIGNORE},
                    strList.toArray());
            assertEquals(mask, Node.NodeFlag.fromStringList(strList));
        }

        {
            final long mask = Node.NodeFlag.DELETE.flagValue;
            List<String> strList = Node.NodeFlag.toStringList(mask);

            assertArrayEquals(
                    new String[]{ApiConsts.FLAG_DELETE},
                    strList.toArray());
            assertEquals(mask, Node.NodeFlag.fromStringList(strList));
        }

        {
            final long mask = 0;
            List<String> strList = Node.NodeFlag.toStringList(mask);

            assertArrayEquals(
                    new String[]{},
                    strList.toArray());
            assertEquals(mask, Node.NodeFlag.fromStringList(strList));
        }
    }
}
