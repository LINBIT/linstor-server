package com.linbit.utils;

import com.linbit.linstor.annotation.Nullable;

import java.util.function.Consumer;
import java.util.function.Function;

public interface Either<L, R>
{
    static <L, R> Either<L, R> left(L left)
    {
        return new Left<>(left);
    }

    static <L, R> Either<L, R> right(R right)
    {
        return new Right<>(right);
    }

    <T> T map(Function<L, T> leftFunction, @Nullable Function<R, T> rightFunction);

    void consume(Consumer<L> leftConsumer, Consumer<R> rightConsumer);

    class Left<L, R> implements Either<L, R>
    {
        private final L left;

        public Left(L leftRef)
        {
            left = leftRef;
        }

        @Override
        public <T> T map(Function<L, T> leftFunction, Function<R, T> rightFunction)
        {
            return leftFunction.apply(left);
        }

        @Override
        public void consume(Consumer<L> leftConsumer, Consumer<R> rightConsumer)
        {
            leftConsumer.accept(left);
        }
    }

    class Right<L, R> implements Either<L, R>
    {
        private final R right;

        public Right(R rightRef)
        {
            right = rightRef;
        }

        @Override
        public <T> T map(Function<L, T> leftFunction, Function<R, T> rightFunction)
        {
            return rightFunction.apply(right);
        }

        @Override
        public void consume(Consumer<L> leftConsumer, Consumer<R> rightConsumer)
        {
            rightConsumer.accept(right);
        }
    }
}
