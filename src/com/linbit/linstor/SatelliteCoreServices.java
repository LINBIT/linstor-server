package com.linbit.linstor;

import com.linbit.fsevent.FileSystemWatch;

/**
 * Core services of the linstor Satellite
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface SatelliteCoreServices extends CoreServices
{
    FileSystemWatch getFsWatch();
}
