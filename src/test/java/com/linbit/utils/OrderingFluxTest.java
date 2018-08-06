package com.linbit.utils;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import static reactor.util.function.Tuples.of;

public class OrderingFluxTest
{
    @Test
    public void testOrderedInOrder()
    {
        Flux<Tuple2<Long, String>> flux = Flux
            .just(of(0L, "a0"), of(1L, "a1"), of(2L, "a2"), of(3L, "a3"))
            .concatWith(Flux.error(new RuntimeException()));

        StepVerifier.create(OrderingFlux.order(flux))
            .expectNext("a0")
            .expectNext("a1")
            .expectNext("a2")
            .expectNext("a3")
            .expectError()
            .verify();
    }

    @Test
    public void testOrderedOutOfOrder()
    {
        Flux<Tuple2<Long, String>> flux = Flux
            .just(of(1L, "foo"), of(0L, "bar"))
            .concatWith(Flux.error(new RuntimeException()));

        StepVerifier.create(OrderingFlux.order(flux))
            .expectNext("bar")
            .expectNext("foo")
            .expectError()
            .verify();
    }

    @Test
    public void testOrderedOutOfOrderAhead()
    {
        Flux<Tuple2<Long, String>> flux = Flux
            .just(of(2L, "foo"), of(0L, "bar"), of(1L, "baz"))
            .concatWith(Flux.error(new RuntimeException()));

        StepVerifier.create(OrderingFlux.order(flux))
            .expectNext("bar")
            .expectNext("baz")
            .expectNext("foo")
            .expectError()
            .verify();
    }
}
