package com.linbit.linstor.core.apicallhandler.controller.backup.nodefinder;

/**
 * This Category is for rscs with layers that prevent a different node from taking over an incremental shipping, such
 * as:<br/>
 * zfs (because the restore will recognize the snapshot is not based on the previous one)<br/>
 * luks (because each volume is encrypted differently, therefore nodes just don't mix)
 */
class CategorySameNode implements Category
{
    @Override
    public int hashCode()
    {
        return CategorySameNode.class.hashCode();
    }

    @Override
    public boolean equals(Object objRef)
    {
        return objRef instanceof CategorySameNode;
    }

    @Override
    public int compareTo(Category other)
    {
        return other.getClass().getSimpleName().compareTo(CategorySameNode.class.getSimpleName());
    }
}
