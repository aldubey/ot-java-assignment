package com.opentext.interview.java.assignment;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.opentext.interview.java.assignment.Main.TaskExecutorImpl.executorService;

public class Main {

    /**
     * Enumeration of task types.
     */
    public enum TaskType {
        READ, WRITE,
    }

    public interface TaskExecutor {
        /**
         * Submit new task to be queued and executed.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @return Future for the task asynchronous computation result.
         */
        <T> Future<T> submitTask(Task<T> task);
    }

    /**
     * Representation of computation to be performed by the {@link TaskExecutor}.
     *
     * @param taskUUID   Unique task identifier.
     * @param taskGroup  Task group.
     * @param taskType   Task type.
     * @param taskAction Callable representing task computation and returning the result.
     * @param <T>        Task computation result value type.
     */
    public record Task<T>(UUID taskUUID, TaskGroup taskGroup, TaskType taskType, Callable<T> taskAction) {
        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    /**
     * Task group.
     *
     * @param groupUUID Unique group identifier.
     */
    public record TaskGroup(UUID groupUUID) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    class TaskExecutorImpl implements TaskExecutor {
        static ExecutorService executorService = Executors.newFixedThreadPool(5);
        private final ReentrantLock reentrantLock = new ReentrantLock();
        private final Condition condition = reentrantLock.newCondition();
        private ConcurrentHashMap<TaskGroup, UUID> runningTaskGroups = new ConcurrentHashMap<>();

        @Override
        public <T> Future<T> submitTask(Task<T> task) {
            Future<T> future = null;
            try {
                reentrantLock.lock();
                while (runningTaskGroups.containsKey(task.taskGroup())) {
                    System.out.println("There is already one task running with the group id " + task.taskGroup().groupUUID());
                    System.out.println("Task with id " + task.taskUUID() + "and group id "+task.taskGroup().groupUUID()+"is waiting for submission");
                    condition.await();
                    System.out.println("Task with id " + task.taskUUID() + "and group id "+task.taskGroup().groupUUID()+"is ready for submission");

                }
                runningTaskGroups.put(task.taskGroup(), task.taskGroup().groupUUID());
                future = executorService.submit(() -> {
                    try {
                        return task.taskAction().call();
                    } finally {
                        reentrantLock.lock();
                        try {
                            runningTaskGroups.remove(task.taskGroup());
                            System.out.println("Task with id " + task.taskUUID() + "and group id "+task.taskGroup().groupUUID()+"is done and remove from list");
                            condition.signalAll();
                        } finally {
                            reentrantLock.unlock();
                        }
                    }
                });
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            } finally {
                reentrantLock.unlock();
            }
            return future;
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Main main = new Main();
        TaskExecutor taskExecutor = main.new TaskExecutorImpl();
        UUID taskGroupUUID1 = UUID.randomUUID();
        UUID taskGroupUUID2 = UUID.randomUUID();
        UUID taskGroupUUID3 = UUID.randomUUID();

        TaskGroup taskGroup1 = new TaskGroup(taskGroupUUID1);
        TaskGroup taskGroup2 = new TaskGroup(taskGroupUUID2);
        TaskGroup taskGroup3 = new TaskGroup(taskGroupUUID3);

        Callable<String> taskAction1Grp1StrUcaseLongRun = () -> main.getLongRunTaskResult("java");
        Callable<String> taskAction2Grp1StrUcaseLongRun = () -> main.getLongRunTaskResult("cat");
        Callable<String> taskAction3Grp2StrUcase = () -> main.getShortRunTaskResult("dog");
        Callable<String> taskAction43Grp2StrUcase = () -> main.getShortRunTaskResult("python");
        Callable<String> taskAction53Grp3StrUcase = () -> main.getShortRunTaskResult("idea");

        Task<String> task1Grp1 = new Task<>(UUID.randomUUID(), taskGroup1, TaskType.READ, taskAction1Grp1StrUcaseLongRun);
        Task<String> task2Grp1 = new Task<>(UUID.randomUUID(), taskGroup1, TaskType.WRITE, taskAction2Grp1StrUcaseLongRun);
        Task<String> task3Grp2 = new Task<>(UUID.randomUUID(), taskGroup2, TaskType.READ, taskAction3Grp2StrUcase);
        Task<String> task4Grp2 = new Task<>(UUID.randomUUID(), taskGroup2, TaskType.WRITE, taskAction43Grp2StrUcase);
        Task<String> task5Grp3 = new Task<>(UUID.randomUUID(), taskGroup3, TaskType.WRITE, taskAction53Grp3StrUcase);

        List<Task<String>> tasks = List.of(task1Grp1, task2Grp1, task3Grp2, task4Grp2, task5Grp3);
        List<Future<String>> futureResults = new ArrayList<>();
        for (Task<String> task : tasks) {
            futureResults.add(taskExecutor.submitTask(task));
        }
        for (Future<String> result : futureResults) {
            System.out.println("Future Result : " + result.get());
        }

        executorService.shutdown();
    }

    private String getLongRunTaskResult(String input) throws InterruptedException {
        Thread.sleep(2000);
        String curThread = Thread.currentThread().getName();
     //   System.out.println("Enter: getLongRunTaskResult for input: " + input + " by " + curThread);
        String result = Optional.ofNullable(input).map(String::toUpperCase).orElse(input);
    //    System.out.println("LongRunTaskResult by thread " + curThread);
     //   System.out.println("Exit: getLongRunTaskResult by " + curThread);
        return result;
    }

    private String getShortRunTaskResult(String input) throws InterruptedException {
        Thread.sleep(1000);
        String curThread = Thread.currentThread().getName();
      //  System.out.println("Enter: getShortRunTaskResult for input: " + input + " by " + curThread);
        String result = Optional.ofNullable(input).map(String::toUpperCase).orElse(input);
      //  System.out.println("ShortRunTaskResult by thread " + curThread);
      //  System.out.println("Exit: getShortRunTaskResult by " + curThread);
        return result;
    }

    /*private ExecutorService getExecutor(){
        // static ExecutorService executorService = Executors.newFixedThreadPool(5);
        int corePoolSize=5;
        int maximumPoolSize=5,
        BlockingQueue<Runnable> workQueue = PriorityBlockingQueue
        static ExecutorService executorService = new ThreadPoolExecutor();
    }*/

}
