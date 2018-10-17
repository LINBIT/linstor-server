package com.linbit.linstor.storage.layer.provider.utils;

import com.linbit.linstor.Volume;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class BatchRunner
{
    public interface Runner
    {
        void run(Volume vlm) throws StorageException, AccessDeniedException, SQLException;
    }

    public static Map<Volume, StorageException> runBatch(
        List<Volume> vlms,
        Runner runner
    )
        throws AccessDeniedException, SQLException
    {
        Map<Volume, StorageException> exceptions = new TreeMap<>();

        for (Volume vlm : vlms)
        {
            try
            {
                runner.run(vlm);
            }
            catch (StorageException exc)
            {
                exceptions.put(vlm, exc);
            }
        }

        return exceptions;
    }

}
