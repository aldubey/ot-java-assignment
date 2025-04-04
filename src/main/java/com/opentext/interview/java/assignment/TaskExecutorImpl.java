package com.opentext.interview.java.assignment;

import com.opentext.interview.java.assignment.Main.Task;
import com.opentext.interview.java.assignment.Main.TaskExecutor;
import com.opentext.interview.java.assignment.Main.TaskGroup;
import com.opentext.interview.java.assignment.Main.TaskType;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TaskExecutorImpl implements TaskExecutor {

    AtomicInteger taskRank = new AtomicInteger(0);
    static Map<String, Long> taskOrderMap = new ConcurrentHashMap<>();
    static ConcurrentLinkedQueue taskQueue = new ConcurrentLinkedQueue<>();
    private final ReentrantLock taskQueueLock = new ReentrantLock();
    private final Condition taskWaitCon = taskQueueLock.newCondition();

    static ExecutorService executorService = Executors.newFixedThreadPool(5);
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private final Condition condition = reentrantLock.newCondition();

    private ConcurrentHashMap<TaskGroup, UUID> runningTaskGroups = new ConcurrentHashMap<>();

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
        Future<T> future = null;
        try {
            taskQueueLock.lock();
            taskQueue.add(task);
        } finally {
            taskQueueLock.unlock();
        }
        handleSameTaskGroups(task);
        future = handleTaskSubmission(task);

        return future;
    }

    /**
     * Submit to executor service and remove taskgroup from CHM which is done
     *
     * @param task
     * @param <T>
     * @return
     */
    private <T> Future<T> handleTaskSubmission(Task<T> task) {
        Future<T> future;
        future = executorService.submit(() -> {
            while (true) {
                try {
                    taskQueueLock.lock();
                    if (taskQueue.peek() == null || task.equals(taskQueue.peek())) {
                        taskQueue.poll();
                        taskWaitCon.signalAll();
                        break;
                    }
                    taskWaitCon.await();
                } finally {
                    taskQueueLock.unlock();
                }

            }
            try {
                return task.taskAction().call();
            } finally {
                reentrantLock.lock();
                try {
                    runningTaskGroups.remove(task.taskGroup());
                    //    System.out.println("Task with id " + task.taskUUID() + "and group id " + task.taskGroup().groupUUID() + "is done and remove from list");
                    condition.signalAll();
                } finally {
                    reentrantLock.unlock();
                }
            }
        });
        return future;
    }

    /**
     * same taskGrop tasks to wait untill the running task completes
     *
     * @param task
     * @param <T>
     */
    private <T> void handleSameTaskGroups(Task<T> task) {
        try {
            reentrantLock.lock();
            while (runningTaskGroups.containsKey(task.taskGroup())) {
                //  System.out.println("There is already one task running with the group id " + task.taskGroup().groupUUID());
                //   System.out.println("Task with id " + task.taskUUID() + "and group id " + task.taskGroup().groupUUID() + "is waiting for submission");

                condition.await();
                //   System.out.println("Task with id " + task.taskUUID() + "and group id " + task.taskGroup().groupUUID() + "is ready for submission");
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } finally {
            reentrantLock.unlock();
        }
        runningTaskGroups.put(task.taskGroup(), task.taskGroup().groupUUID());
    }


    private String getLongRunTaskResult(String input) throws InterruptedException {
        taskOrderMap.put(input, System.nanoTime());
        System.out.println("Task " + input + " Start Time: " + Instant.now());
        Thread.sleep(2000);
        System.out.println("Task " + input + " End Time: " + Instant.now());
        return Optional.ofNullable(input).map(String::toUpperCase).orElse(input);
    }

    private String getShortRunTaskResult(String input) throws InterruptedException {
        taskOrderMap.put(input, System.nanoTime());
        System.out.println("Task " + input + " Start Time: " + Instant.now());
        //Thread.sleep(4000);
        System.out.println("Task " + input + " End Time: " + Instant.now());
        return Optional.ofNullable(input).map(String::toUpperCase).orElse(input);
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TaskExecutorImpl taskExecutor = new TaskExecutorImpl();
        UUID taskGroupUUID1 = UUID.randomUUID();
        UUID taskGroupUUID2 = UUID.randomUUID();
        UUID taskGroupUUID3 = UUID.randomUUID();

        TaskGroup taskGroup1 = new TaskGroup(taskGroupUUID1);
        TaskGroup taskGroup2 = new TaskGroup(taskGroupUUID2);
        TaskGroup taskGroup3 = new TaskGroup(taskGroupUUID3);

        Callable<String> taskAction1Grp1StrUcase = () -> taskExecutor.getLongRunTaskResult("java-task-1");
        Callable<String> taskAction2Grp1StrUcase = () -> taskExecutor.getShortRunTaskResult("cat-task-2");
        Callable<String> taskAction3Grp2StrUcase = () -> taskExecutor.getShortRunTaskResult("dog-task-3");
        Callable<String> taskAction43Grp2StrUcase = () -> taskExecutor.getLongRunTaskResult("python-task-4");
        Callable<String> taskAction53Grp3StrUcase = () -> taskExecutor.getShortRunTaskResult("idea-task-5");

        Task<String> task1Grp1 = new Task<>(UUID.randomUUID(), taskGroup1, TaskType.READ, taskAction1Grp1StrUcase);
        Task<String> task2Grp1 = new Task<>(UUID.randomUUID(), taskGroup1, TaskType.WRITE, taskAction2Grp1StrUcase);
        Task<String> task3Grp2 = new Task<>(UUID.randomUUID(), taskGroup2, TaskType.READ, taskAction3Grp2StrUcase);
        Task<String> task4Grp2 = new Task<>(UUID.randomUUID(), taskGroup2, TaskType.WRITE, taskAction43Grp2StrUcase);
        Task<String> task5Grp3 = new Task<>(UUID.randomUUID(), taskGroup3, TaskType.WRITE, taskAction53Grp3StrUcase);

        List<Task<String>> tasks = List.of(task1Grp1, task2Grp1, task3Grp2, task4Grp2, task5Grp3);
        List<Future<String>> futureResults = new ArrayList<>();
        for (Task<String> task : tasks) {
            futureResults.add(taskExecutor.submitTask(task));
        }
        for (Future<String> result : futureResults) {
            // System.out.println("Future Result : " + result.get());
            result.get();
        }
        System.out.println("\n\n");
        System.out.println("----------Order of Task Start in nanoTime--------- \n");
        taskOrderMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getValue)).forEach(e -> {
            System.out.println("@@@@ Task for input: " + e.getKey() + " start time: " + e.getValue());
        });

        executorService.shutdown();
    }

}
