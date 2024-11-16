package com.github.darwindemian.springtools;

public interface ThreadAllocationStrategy {
    int getMaxThreads();

    int getMinThreads();

    void onThreadPoolExhausted() throws Exception;
}
