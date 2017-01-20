package com.linbit.drbdmanage.storage;

import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import java.util.Map;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class LvmThinDriver implements StorageDriver
{
    public static final String LVM_CREATE = "lvcreate";
    public static final String LVM_DELETE = "lvremove";
    public static final String LVM_LVS = "lvs";
    public static final String LVM_VGS = "vgs";

    @Override
    public void startVolume(String identifier)
        throws StorageException
    {
        // TODO: Implement
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void stopVolume(String identifier)
        throws StorageException
    {
        // TODO: Implement
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String createVolume(String identifier, long size)
        throws StorageException, MaxSizeException, MinSizeException
    {
        // TODO: Implement
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteVolume(String identifier)
        throws StorageException
    {
        // TODO: Implement
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void checkVolume(String identifier, long size)
        throws StorageException
    {
        // TODO: Implement
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getVolumePath(String identifier)
        throws StorageException
    {
        // TODO: Implement
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getSize(String identifier)
        throws StorageException
    {
        // TODO: Implement
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, String> getTraits()
    {
        // TODO: Implement
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setConfiguration(Map<String, String> config)
        throws StorageException
    {
        // TODO: Implement
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
