package com.linbit.linstor;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Node;

import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 *
 * @author rp
 */
public class NodeTest
{
    @Test
    public void testAllFlags()
    {
        final long mask = Node.Flags.DELETE.flagValue |
                Node.Flags.QIGNORE.flagValue |
            Node.Flags.EVICTED.flagValue |
            Node.Flags.EVACUATE.flagValue;
        List<String> strList = Node.Flags.toStringList(mask);
        assertEquals(Node.Flags.values().length, strList.size());

        assertArrayEquals(
            new String[]
            {
                ApiConsts.FLAG_DELETE,
                ApiConsts.FLAG_EVICTED,
                ApiConsts.FLAG_EVACUATE,
                ApiConsts.FLAG_QIGNORE  },
                strList.toArray());
        assertEquals(mask, Node.Flags.fromStringList(strList));
    }

    @Test
    public void testFlags()
    {
        {
            final long mask = Node.Flags.DELETE.flagValue |
                    Node.Flags.QIGNORE.flagValue;
            List<String> strList = Node.Flags.toStringList(mask);

            assertArrayEquals(
                    new String[]{ApiConsts.FLAG_DELETE, ApiConsts.FLAG_QIGNORE},
                    strList.toArray());
            assertEquals(mask, Node.Flags.fromStringList(strList));
        }

        {
            final long mask = Node.Flags.DELETE.flagValue;
            List<String> strList = Node.Flags.toStringList(mask);

            assertArrayEquals(
                    new String[]{ApiConsts.FLAG_DELETE},
                    strList.toArray());
            assertEquals(mask, Node.Flags.fromStringList(strList));
        }

        {
            final long mask = 0;
            List<String> strList = Node.Flags.toStringList(mask);

            assertArrayEquals(
                    new String[]{},
                    strList.toArray());
            assertEquals(mask, Node.Flags.fromStringList(strList));
        }
    }
}
