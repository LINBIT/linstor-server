package com.linbit.linstor.core.apicallhandler.controller.backup.nodefinder;

class CategoryZfs implements Category
{
    @Override
    public int hashCode()
    {
        return CategoryZfs.class.hashCode();
    }

    @Override
    public boolean equals(Object objRef)
    {
        return objRef instanceof CategoryZfs;
    }

    @Override
    public int compareTo(Category other)
    {
        return other.getClass().getSimpleName().compareTo(CategoryZfs.class.getSimpleName());
    }
}
