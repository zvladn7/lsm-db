package ru.spbstu.service;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/**
 * Implement this interface for processing http requests.
 */
@FunctionalInterface
public interface LocalExecutor<T> {

    /**
     * Execute local request and return response.
     */
    @NotNull
    CompletableFuture<T> execute();

}
