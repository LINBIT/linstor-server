package com.linbit.drbdmanage;

import com.linbit.fsevent.FileSystemWatch;

/**
 * Core services of the drbdmanage Satellite
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface SatelliteCoreServices extends CoreServices
{
    FileSystemWatch getFsWatch();
}
