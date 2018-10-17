package com.linbit.linstor.storage2.layer.data.categories;

/**
 * Marker interface to ensure type safety
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VlmLayerData extends LayerData
{
    boolean exists();

    boolean isFailed();

    long getUsableSize();

    long getAllocatedSize();
}
