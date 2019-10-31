package iibench.threads;

public class ThreadUtils {
    public void joinThread(final Thread thread) throws InterruptedException {
        if (thread.isAlive()) {
            thread.join();
        }
    }

    public void joinThreads(final Thread[] threads, final int writerThreads) throws InterruptedException {
        for (int i = 0; i < writerThreads; i++) {
            joinThread(threads[i]);
        }
    }

    public void waitForMs(int timeToWaitInMs) {
        try {
            Thread.sleep(timeToWaitInMs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
