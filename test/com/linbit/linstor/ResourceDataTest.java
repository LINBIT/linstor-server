package com.linbit.linstor;

import com.linbit.linstor.api.ApiConsts;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author rpeinthor
 */
public class ResourceDataTest
{
    @Test
    public void testAllFlags()
    {
        final long mask = Resource.RscFlags.CLEAN.flagValue |
                Resource.RscFlags.DELETE.flagValue |
                Resource.RscFlags.DISKLESS.flagValue;
        List<String> strList = Resource.RscFlags.toStringList(mask);
        assertEquals(Resource.RscFlags.values().length, strList.size());

        assertArrayEquals(
                new String[]{ApiConsts.FLAG_CLEAN, ApiConsts.FLAG_DELETE, ApiConsts.FLAG_DISKLESS},
                strList.toArray());
        assertEquals(mask, Resource.RscFlags.fromStringList(strList));
    }

    @Test
    public void testFlags()
    {
        {
            final long mask = Resource.RscFlags.CLEAN.flagValue |
                    Resource.RscFlags.DISKLESS.flagValue;
            List<String> strList = Resource.RscFlags.toStringList(mask);

            assertArrayEquals(
                    new String[]{ApiConsts.FLAG_CLEAN, ApiConsts.FLAG_DISKLESS},
                    strList.toArray());
            assertEquals(mask, Resource.RscFlags.fromStringList(strList));
        }

        {
            final long mask = 0;
            List<String> strList = Resource.RscFlags.toStringList(mask);

            assertArrayEquals(
                    new String[]{},
                    strList.toArray());
            assertEquals(mask, Resource.RscFlags.fromStringList(strList));
        }
    }
}
