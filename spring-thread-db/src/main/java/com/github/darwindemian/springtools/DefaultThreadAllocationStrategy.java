package com.github.darwindemian.springtools;

import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

@Component
public class DefaultThreadAllocationStrategy implements ThreadAllocationStrategy {

    // TODO: control how many threads are running for each Pool
    // TODO: control thread exhaustion

    @Value("${springtools.springdbthreadrunner.max_threads:100}")
    private int maxThreads = 100;

    @Value("${springtools.springdbthreadrunner.min_threads:1}")
    private int minThreads = 1;

    @Override
    public int getMaxThreads() {
        return maxThreads;
    }

    @Override
    public int getMinThreads() {
        return minThreads;
    }

    @Override
    public void onThreadPoolExhausted() throws IllegalAccessException {
        throw new IllegalAccessException("Thread pool exhausted!");
    }
}
