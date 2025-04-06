package com.opentext.interview.java.assignment;

import com.opentext.interview.java.assignment.Main.Task;
import com.opentext.interview.java.assignment.Main.TaskExecutor;
import com.opentext.interview.java.assignment.Main.TaskGroup;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.*;

public class TaskExecutorImplConcurrent implements TaskExecutor {
    public static final String JAVA_TASK_1 = "java-task-1";
    public static final String CAT_TASK_2 = "cat-task-2";
    public static final String DOG_TASK_3 = "dog-task-3";
    public static final String PYTHON_TASK_4 = "python-task-4";
    public static final String IDEA_TASK_5 = "idea-task-5";
    AtomicLong submissionCounter = new AtomicLong(0);
    AtomicLong executionCounter = new AtomicLong(0);
    static final Map<String, Long> taskOrderMap = new ConcurrentHashMap<>();
    static final Map<String, Long> taskSubmissionOrderMap = new ConcurrentHashMap<>();
    static final Map<String, Long> taskExceOrderMap = new ConcurrentHashMap<>();

    static ConcurrentLinkedQueue taskQueue = new ConcurrentLinkedQueue<>();
    private final ReentrantLock taskQueueLock = new ReentrantLock();
    private final Condition taskWaitCon = taskQueueLock.newCondition();

    static ExecutorService executorService = Executors.newFixedThreadPool(5);
    private final ReadWriteLock taskGroupReadWriteLock = new ReentrantReadWriteLock();
    private final Lock taskGroupWriteLock = taskGroupReadWriteLock.writeLock();
    private final Condition writeCondition = taskGroupWriteLock.newCondition();


    private ConcurrentHashMap<TaskGroup, UUID> runningTaskGroups = new ConcurrentHashMap<>();

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
        UpperCaseTask2 upperCaseTask2 = (UpperCaseTask2) task.taskAction();
        Future<T> future = null;
        try {
            taskQueueLock.lock();
            taskSubmissionOrderMap.put(upperCaseTask2.getTaskName(), submissionCounter.incrementAndGet());
            System.out.println("Task added in queue: " + upperCaseTask2.getTaskName());
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
        UpperCaseTask2 upperCaseTask2 = (UpperCaseTask2) task.taskAction();
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
                    System.out.println("***** handleTaskSubmission Waiting task " + upperCaseTask2.getTaskName());
                } finally {
                    taskQueueLock.unlock();
                }

            }
            try {

                taskExceOrderMap.put(upperCaseTask2.getTaskName(), executionCounter.incrementAndGet());
                return task.taskAction().call();
            } finally {
                taskGroupWriteLock.lock();
                try {

                    runningTaskGroups.remove(task.taskGroup());
                    writeCondition.signalAll();
                } finally {
                    taskGroupWriteLock.unlock();
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
            taskGroupWriteLock.lock();
            while (runningTaskGroups.containsKey(task.taskGroup())) {
                writeCondition.await();
            }
            runningTaskGroups.put(task.taskGroup(), task.taskGroup().groupUUID());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            taskGroupWriteLock.unlock();
        }
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
        TaskExecutorImplConcurrent taskExecutor = new TaskExecutorImplConcurrent();
        UUID taskGroupUUID1 = UUID.randomUUID();
        UUID taskGroupUUID2 = UUID.randomUUID();
        UUID taskGroupUUID3 = UUID.randomUUID();

        TaskGroup taskGroup1 = new TaskGroup(taskGroupUUID1);
        TaskGroup taskGroup2 = new TaskGroup(taskGroupUUID2);
        TaskGroup taskGroup3 = new TaskGroup(taskGroupUUID3);

        Main.Task<String> task1Grp1 = new Main.Task<>(UUID.randomUUID(), taskGroup1, Main.TaskType.READ, new UpperCaseTask2(JAVA_TASK_1, 2000));
        Main.Task<String> task2Grp1 = new Main.Task<>(UUID.randomUUID(), taskGroup1, Main.TaskType.WRITE, new UpperCaseTask2(CAT_TASK_2, 1000));
        Main.Task<String> task3Grp2 = new Main.Task<>(UUID.randomUUID(), taskGroup2, Main.TaskType.READ, new UpperCaseTask2(DOG_TASK_3, 1500));
        Main.Task<String> task4Grp2 = new Main.Task<>(UUID.randomUUID(), taskGroup2, Main.TaskType.WRITE, new UpperCaseTask2(PYTHON_TASK_4, 2000));
        Main.Task<String> task5Grp3 = new Main.Task<>(UUID.randomUUID(), taskGroup3, Main.TaskType.WRITE, new UpperCaseTask2(IDEA_TASK_5, 500));


        List<Task<String>> tasks = List.of(task1Grp1, task2Grp1, task3Grp2, task4Grp2, task5Grp3);
        List<Future<Future<String>>> futures = new ArrayList<>();

        ExecutorService taskSubmitter = Executors.newFixedThreadPool(10);
        //
        for (Task<String> task : tasks) {
            Future<Future<String>> f = taskSubmitter.submit(() -> taskExecutor.submitTask(task));
            futures.add(f);
        }
        for (Future<Future<String>> f : futures) {
            f.get().get();
        }
        System.out.println("\n\n");
        System.out.println("----------Order of Task Start in nanoTime--------- \n");
        taskSubmissionOrderMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getValue)).forEach(e -> {
            System.out.println("@@@@ Task submitted in this " + e.getKey());
        });

        System.out.println("\n\n");

        taskExceOrderMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getValue)).forEach(e -> {
            System.out.println("### Task executed in this " + e.getKey());
        });
        taskSubmitter.shutdown();
        executorService.shutdown();
    }

}

class UpperCaseTask2 implements Callable<String> {
    private final String taskName;
    private final int delayMillis;

    public UpperCaseTask2(String taskName, int delayMillis) {
        this.taskName = Objects.requireNonNull(taskName);
        this.delayMillis = delayMillis;
    }

    @Override
    public String call() throws Exception {
        Thread.sleep(delayMillis);
        return taskName.toUpperCase();
    }

    public String getTaskName() {
        return taskName;
    }
}
