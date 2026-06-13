package rxmini;

public interface Scheduler {
    void execute(Runnable task);
}
