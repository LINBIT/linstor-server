package com.linbit.linstor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Resource;

import java.util.List;

import org.junit.Test;

/**
 *
 * @author rpeinthor
 */
public class ResourceTest
{
    @Test
    public void testAllFlags()
    {
        final long mask = Resource.Flags.CLEAN.flagValue |
            Resource.Flags.DELETE.flagValue |
            Resource.Flags.DISKLESS.flagValue |
            Resource.Flags.DISK_ADD_REQUESTED.flagValue |
            Resource.Flags.DISK_ADDING.flagValue |
            Resource.Flags.DISK_REMOVE_REQUESTED.flagValue |
            Resource.Flags.DISK_REMOVING.flagValue |
            Resource.Flags.TIE_BREAKER.flagValue;
        List<String> strList = Resource.Flags.toStringList(mask);
        assertEquals(Resource.Flags.values().length, strList.size());

        assertArrayEquals(
                new String[]{
                    ApiConsts.FLAG_CLEAN,
                    ApiConsts.FLAG_DELETE,
                    ApiConsts.FLAG_DISKLESS,
                    ApiConsts.FLAG_DISK_ADD_REQUESTED,
                    ApiConsts.FLAG_DISK_ADDING,
                    ApiConsts.FLAG_DISK_REMOVE_REQUESTED,
                    ApiConsts.FLAG_DISK_REMOVING,
                    ApiConsts.FLAG_TIE_BREAKER
                },
                strList.toArray());
        assertEquals(mask, Resource.Flags.fromStringList(strList));
    }

    @Test
    public void testFlags()
    {
        {
            final long mask = Resource.Flags.CLEAN.flagValue |
                Resource.Flags.DISKLESS.flagValue;
            List<String> strList = Resource.Flags.toStringList(mask);

            assertArrayEquals(
                new String[]
                {
                    ApiConsts.FLAG_CLEAN, ApiConsts.FLAG_DISKLESS
                },
                strList.toArray()
            );
            assertEquals(mask, Resource.Flags.fromStringList(strList));
        }

        {
            final long mask = 0;
            List<String> strList = Resource.Flags.toStringList(mask);

            assertArrayEquals(
                new String[]
                {},
                strList.toArray()
            );
            assertEquals(mask, Resource.Flags.fromStringList(strList));
        }
    }
}
