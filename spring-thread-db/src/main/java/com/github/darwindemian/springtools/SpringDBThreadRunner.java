package com.github.darwindemian.springtools;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Provides a stream-lined way of doing MultiThreading with error handling, transaction management, when dealing with
 * database operations.
 */
@Service
public class SpringDBThreadRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringDBThreadRunner.class);

    @Autowired
    private PlatformTransactionManager transactionManager;

    public static final int MAX_THREADS = Integer.MAX_VALUE;
    public static final int MIN_THREADS = 1;

    /**
     * Facilitates controlling timeout when referring to this class
     */
    public static class Timeout {
        public final long timeout;
        public final TimeUnit timeUnit;

        public Timeout(long timeout, TimeUnit timeUnit) {
            this.timeout = timeout;
            this.timeUnit = Optional.ofNullable(timeUnit).orElse(TimeUnit.MILLISECONDS);
        }

        public static Timeout await(long timeout, TimeUnit timeUnit) {
            return new Timeout(timeout, timeUnit);
        }
    }

    /**
     * Provides a way to debug and control the tasks ran.
     * You can check the result of each task here, including what exceptions it has thrown, and if it returned something.
     * <p>
     * Use your Task ID (the index of your list of tasks: {@code listOfCallables.indexOf(yourCallable)}) to check the results by its ID, or use {@link Result#getResults()} / {@link Result#getExceptions()}
     * to get everything.
     * The last ID ({@code task.size() + 1}) is always reserved to errors that weren't thrown by any of your tasks.
     * You can assume that every 'results/exceptions' will always have all IDs initialized.
     * </p>
     * <p>You can check general status as well:</p>
     * <ul>
     *     <li>
     *         {@link Result#finished}: based on whether, if asked to await termination, it actually finished before timing out.
     *         Async tasks will never have a finished state.
     *     </li>
     *     <li>
     *         {@link Result#tasksRan}: tasks that completed their lifecycles, no matter if they threw an exception or returned a result.
     *         This will only not be counted if the task was stopped while running/beforehand, i.e: in the case of a Timeout, shutdown request,
     *         or a thread being interrupted by a task that ran beforehand in that same thread.
     *     </li>
     *     <li>
     *         {@link Result#numOfTasks}: based on the exact number of Callables passed.
     *     </li>
     *     <li>
     *         {@link Result#await}: whether you provided a Timeout or not.
     *     </li>
     * </ul>
     *
     * <p>
     *     Although all maps here are thread-safe, the result lists provided might or not be completely thread-safe.
     *     Async tasks results aren't promised to be thread-safe, unless you use a
     *     <a href="https://stackoverflow.com/questions/11360401/java-synchronized-list#11360516">synchronized list iterator</a>.
     *     Synced tasks results are always thread-safe, you will be provided a copy of the list if all tasks haven't finished,
     *     or use {@link Result#getResultsByTask()} / {@link Result#getExceptionsByTask()} to get a non-thread-safe/live version of the lists.
     * </p>
     *
     * @param <T> {@link Result#results} return type, tied to the return type of your tasks
     */
    public static class Result<T> {
        private final Map<Integer, List<Exception>> exceptions = new ConcurrentHashMap<>();
        private final Map<Integer, List<T>> results = new ConcurrentHashMap<>();
        public boolean hasException = false;

        public boolean finished = false;
        public final AtomicLong tasksRan = new AtomicLong(0);
        public final int numOfTasks;
        public final boolean await;

        public Result(boolean await, int numOfTasks) {
            this.await = await;
            this.numOfTasks = numOfTasks;

            // init lists so you don't get a NullPointerException
            for (int i = 0; i < numOfTasks; i++) {
                exceptions.put(i, Collections.synchronizedList(new ArrayList<>()));
                results.put(i, Collections.synchronizedList(new ArrayList<>()));
            }

            // Internal Thread task index, this is what is used when an error occurs outside a running task
            results.put(numOfTasks + 1, Collections.synchronizedList(new ArrayList<>()));
        }

        /**
         * @return thread-safe map of all exceptions by task. The list included is not promised to be thread-safe,
         * use {@link Result#getExceptionsByTask(Integer)} to assure thread safety.
         */
        public Map<Integer, List<Exception>> getExceptionsByTask() {
            return exceptions;
        }

        /**
         * @return thread-safe list of all exceptions by task.
         */
        public List<Exception> getExceptionsByTask(Integer task) {
            return getList(exceptions.get(task));
        }

        /**
         * @return thread-safe map of all results by task. The list included is not promised to be thread-safe,
         * use {@link Result#getResultsByTask(Integer)} to assure thread safety.
         */
        public Map<Integer, List<T>> getResultsByTask() {
            return results;
        }

        /**
         * @return thread-safe list of all results by task.
         */
        public List<T> getResultsByTask(Integer task) {
            return getList(results.get(task));
        }

        /**
         * @return thread-safe list of all exceptions.
         */
        public List<Exception> getExceptions() {
            return getMapList(exceptions);
        }

        /**
         * @return exceptions not thrown inside any thread.
         */
        public List<Exception> getInternalExceptions() {
            return getExceptionsByTask(numOfTasks + 1);
        }

        /**
         * @return thread-safe list of all results.
         */
        public List<T> getResults() {
            return getMapList(results);
        }

        private <K> List<K> getMapList(Map<Integer, List<K>> mapList) {
            List<K> list = mapList.values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

            return getList(list);
        }

        private <K> List<K> getList(List<K> list) {
            return await && !finished ? Collections.unmodifiableList(list) : list;
        }

        public void logResults(Logger logger) {
            logger.debug("results: " + getResults().size());
            logger.debug("tasksRan: " + tasksRan);
            logger.debug("finished: " + finished);
            logger.debug("unfinished: " + getExceptions().size());
        }

    }

    /**
     * Runs tasks with {@link MainThreadContext#securityContext}, and assures correct rollback of all tasks if necessary.
     * Use {@link SpringDBThreadRunner#run(List, int, boolean, Timeout, Long)} when running async tasks (without {@code timeout}).
     *
     * @param tasks your list of tasks that will run.
     * @param threads number of threads between {@link SpringDBThreadRunner#MIN_THREADS} and {@link SpringDBThreadRunner#MAX_THREADS}.
     * @param terminateOnException if you want to roll back and stop all tasks on the first error encountered.
     * @param timeout whether you want the operation to time out for this amount.
     * @param throttle whether you want the next task to sleep for this amount.
     * @return summarized result of tasks
     * @param <T> task result type
     * @see SpringDBThreadRunner#run(List, int, boolean, Timeout, Long)
     */
    public <T> AtomicReference<Result<T>> runWithMasterTransaction(
            @NotNull List<Callable<T>> tasks, int threads, boolean terminateOnException, @NotNull Timeout timeout,
            @Nullable Long throttle
    ) {
        SecurityContext securityContext = SecurityContextHolder.getContext();
        TransactionTemplate masterTransaction = new TransactionTemplate(transactionManager);
        return masterTransaction.execute(masterStatus -> {
            AtomicReference<Result<T>> result = run(tasks, threads, terminateOnException, timeout, throttle, securityContext);

            boolean mustTerminate = terminateOnException && result.get().hasException;
            boolean notFinished = terminateOnException && timeout != null && !result.get().finished;
            if (mustTerminate || notFinished) masterStatus.setRollbackOnly();
            return result;
        });
    }

    /**
     * Runs tasks with {@link MainThreadContext#securityContext}, with no guarantee of correct individual rollback behaviour.
     * Use this when running async tasks (without {@code timeout}).
     *
     * @param tasks your list of tasks that will run.
     * @param threads number of threads between {@link SpringDBThreadRunner#MIN_THREADS} and {@link SpringDBThreadRunner#MAX_THREADS}.
     * @param terminateOnException if you want to roll back and stop all tasks on the first error encountered. Complete rollback and stopping not guaranteed.
     * @param timeout whether you want the operation to time out for this amount.
     * @param throttle whether you want the next task to sleep for this amount.
     * @return summarized result of tasks
     * @param <T> task result type
     * @see SpringDBThreadRunner#runWithMasterTransaction(List, int, boolean, Timeout, Long)
     */
    public <T> AtomicReference<Result<T>> run(
            @NotNull List<Callable<T>> tasks, int threads, boolean terminateOnException, @Nullable Timeout timeout,
            @Nullable Long throttle
    ) {
        SecurityContext securityContext = SecurityContextHolder.getContext();
        return run(tasks, threads, terminateOnException, timeout, throttle, securityContext);
    }

    /**
     * Run tasks with no guarantee of correct individual rollback behaviour.
     * Use this when running async tasks (without {@code timeout}).
     *
     * @param tasks your list of tasks that will run.
     * @param threads number of threads between {@link SpringDBThreadRunner#MIN_THREADS} and {@link SpringDBThreadRunner#MAX_THREADS}.
     * @param terminateOnException if you want to roll back and stop all tasks on the first error encountered. Complete rollback and task stopping not guaranteed.
     * @param timeout whether you want the operation to time out for this amount.
     * @param throttle whether you want the next task to sleep for this amount.
     * @param securityContext necessary to ensure DB operations. Use {@code SecurityContextHolder.getContext()} in the main thread,
     *                        {@link MainThreadContext#securityContext} to get it directly,
     *                        or {@link MainThreadContext#wrapExecutionContext(Callable)} in your tasks.
     * @return summarized result of tasks
     * @param <T> task result type
     * @see SpringDBThreadRunner#runWithMasterTransaction(List, int, boolean, Timeout, Long)
     */
    public <T> AtomicReference<Result<T>> run(
            @NotNull List<Callable<T>> tasks, int threads, boolean terminateOnException, @Nullable Timeout timeout,
            @Nullable Long throttle, @Nullable SecurityContext securityContext
    ) {
        AtomicReference<Result<T>> result = new AtomicReference<>(new Result<>(timeout != null, tasks.size()));

        ExecutorService ex = Executors.newFixedThreadPool(
                Math.max(MIN_THREADS, Math.min(threads, MAX_THREADS))
        );

        for (Callable<T> task : tasks) {
            ex.submit(() -> {
                if (securityContext != null) SecurityContextHolder.setContext(securityContext);
                TransactionTemplate threadTransaction = new TransactionTemplate(transactionManager);
                threadTransaction.executeWithoutResult(status -> {
                    runTask(tasks, terminateOnException, throttle, task, result, ex);
                    if (terminateOnException && result.get().hasException) status.setRollbackOnly();
                });
            });
        }

        ex.shutdown();
        if (timeout != null) {
            try {
                result.get().finished = ex.awaitTermination(timeout.timeout, timeout.timeUnit);
            } catch (InterruptedException e) {
                result.get().hasException = true;
                addToMap(result.get().exceptions, result.get().numOfTasks + 1, e);
            }

            if (!result.get().finished) ex.shutdownNow();
        }

        return result;
    }

    private static <T> void runTask(
            List<Callable<T>> tasks, boolean terminateOnException, Long throttle, Callable<T> task,
            AtomicReference<Result<T>> result, ExecutorService ex
    ) {
        Thread thread = Thread.currentThread();
        int taskIndex = tasks.indexOf(task);
        // Protection in case the task can't be stopped yet, but you asked it to
        if (thread.isInterrupted() || (terminateOnException && result.get().hasException)) {
            LOGGER.debug("Stopped beforehand: " + thread.getId() + " " + taskIndex);
            return;
        }

        try {
            T taskResult = task.call();
            result.get().results.get(taskIndex).add(taskResult);
        } catch (Exception e) {
            handleException(taskIndex, terminateOnException, result, ex, e, thread);
        } finally {
            if (throttle != null) {
                try {
                    Thread.sleep(throttle);
                } catch (InterruptedException e) {
                    handleException(taskIndex, terminateOnException, result, ex, e, thread);
                }
            }

            LOGGER.debug("Ran: " + thread.getId() + " " + taskIndex);
            result.get().tasksRan.getAndIncrement();
        }
    }

    private static <T> void handleException(
            int taskIndex, boolean terminateOnException, AtomicReference<Result<T>> result,
            ExecutorService ex, Exception e, Thread thread
    ) {
        result.get().hasException = true;
        if (e instanceof InterruptedException) {
            LOGGER.debug("Interrupted: " + thread.getId() + " " + taskIndex);
        }

        result.get().exceptions.get(taskIndex).add(e);
        if (terminateOnException) {
            LOGGER.debug("Shutdown now: " + thread.getId() + " " + taskIndex);
            ex.shutdownNow();
            thread.interrupt();
        }
    }
}
