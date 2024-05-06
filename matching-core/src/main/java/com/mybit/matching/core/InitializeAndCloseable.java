package com.mybit.matching.core;

public interface InitializeAndCloseable {
    default boolean init() {
        return true;
    }

    boolean startup();

    default boolean flush() {
        return true;
    }

    default boolean close() {
        return true;
    }

}
