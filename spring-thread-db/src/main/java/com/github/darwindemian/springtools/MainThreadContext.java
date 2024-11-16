package com.github.darwindemian.springtools;

import org.jetbrains.annotations.NotNull;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.concurrent.Callable;

/**
 * Facilitates holding, and wrapping an execution context with the main thread {@link SecurityContext}
 */
public class MainThreadContext {
    // Use this if you want the statically defined context
    public static final SecurityContext securityContext = SecurityContextHolder.getContext();

    /**
     * Use this when you want to wrap your multithreaded task in the main thread context.
     * @param task your multi-threaded task
     * @return task with the main thread context
     * @param <T> your task return type
     */
    @NotNull
    public static <T> Callable<T> wrapExecutionContext(Callable<T> task) {
        SecurityContext context = SecurityContextHolder.getContext();
        return () -> {
            SecurityContextHolder.setContext(context);
            return task.call();
        };
    }
}
