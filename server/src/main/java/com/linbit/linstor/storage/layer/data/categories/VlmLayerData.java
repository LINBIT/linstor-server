package com.linbit.linstor.storage.layer.data.categories;

import com.linbit.linstor.storage.layer.data.State;

import java.util.List;

/**
 * Marker interface to ensure type safety
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VlmLayerData extends LayerData
{
    enum Size
    {
        TOO_SMALL,
        TOO_LARGE,
        TOO_LARGE_WITHIN_TOLERANCE,
        AS_EXPECTED
    }

    boolean exists();

    boolean isFailed();

    List<? extends State> getStates();
}
