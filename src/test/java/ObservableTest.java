import org.junit.jupiter.api.Test;
import rxmini.Disposable;
import rxmini.Observable;
import rxmini.Observer;
import rxmini.schedulers.ComputationScheduler;
import rxmini.schedulers.IOThreadScheduler;
import rxmini.schedulers.SingleThreadScheduler;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class ObservableTest {

    @Test
    void testMapAndFilter() {
        List<Integer> result = new ArrayList<>();

        Observable<Integer> source = Observable.create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        });

        source
                .map(x -> x * 10)
                .filter(x -> x >= 20)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        result.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Unexpected error: " + t);
                    }

                    @Override
                    public void onComplete() {}
                });

        assertEquals(List.of(20, 30), result);
    }

    @Test
    void testFlatMap() {
        List<String> result = new ArrayList<>();

        Observable<Integer> source = Observable.create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        });

        source
                .flatMap(x -> Observable.<String>create(e -> {
                    e.onNext("inner-" + x);
                    e.onComplete();
                }))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        result.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Unexpected error: " + t);
                    }

                    @Override
                    public void onComplete() {}
                });

        assertEquals(List.of("inner-1", "inner-2"), result);
    }

    @Test
    void testErrorPropagation() {
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        Observable<Integer> source = Observable.create(emitter -> {
            emitter.onNext(1);
            emitter.onError(new RuntimeException("boom"));
        });

        source.subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {}

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
            }

            @Override
            public void onComplete() {}
        });

        assertNotNull(errorRef.get());
        assertEquals("boom", errorRef.get().getMessage());
    }

    @Test
    void testDisposableStopsEvents() {
        List<Integer> result = new ArrayList<>();

        Observable<Integer> source = Observable.create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        });

        Disposable[] d = new Disposable[1];

        d[0] = source.subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
                result.add(item);
                if (item == 2) {
                    d[0].dispose();
                }
            }

            @Override
            public void onError(Throwable t) {}

            @Override
            public void onComplete() {}
        });

        assertEquals(List.of(1, 2), result);
        assertTrue(d[0].isDisposed());
    }

    @Test
    void testSubscribeOnChangesThread() throws InterruptedException {
        IOThreadScheduler io = new IOThreadScheduler();
        AtomicReference<String> threadName = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Integer> source = Observable.create(emitter -> {
            threadName.set(Thread.currentThread().getName());
            emitter.onNext(1);
            emitter.onComplete();
            latch.countDown();
        });

        source
                .subscribeOn(io)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {}

                    @Override
                    public void onError(Throwable t) {
                        fail("Unexpected error: " + t);
                    }

                    @Override
                    public void onComplete() {}
                });

        latch.await();
        assertTrue(threadName.get().toLowerCase().contains("pool"));
    }

    @Test
    void testObserveOnChangesThread() throws InterruptedException {
        SingleThreadScheduler single = new SingleThreadScheduler();
        AtomicReference<String> threadName = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Integer> source = Observable.create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        });

        source
                .observeOn(single)
                .flatMap(x -> Observable.<String>create(e -> {
                    e.onNext("value: " + x);
                    e.onComplete();
                }))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        threadName.set(Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Unexpected error: " + t);
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        latch.await();
        assertNotNull(threadName.get());
        assertTrue(threadName.get().toLowerCase().contains("single"));
    }

    @Test
    void testComputationScheduler() throws InterruptedException {
        ComputationScheduler comp = new ComputationScheduler();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();

        Observable<Integer> source = Observable.create(emitter -> {
            emitter.onNext(42);
            emitter.onComplete();
        });

        source
                .observeOn(comp)
                .flatMap(x -> Observable.<String>create(e -> {
                    e.onNext("value: " + x);
                    e.onComplete();
                }))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        threadName.set(Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Unexpected error: " + t);
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        latch.await();
        assertNotNull(threadName.get());
        assertTrue(threadName.get().toLowerCase().contains("pool"));
    }
}