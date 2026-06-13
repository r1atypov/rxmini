package rxmini;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

public class Observable<T> {

    @FunctionalInterface
    public interface OnSubscribe<T> {
        void subscribe(Observer<? super T> observer);
    }

    private final OnSubscribe<T> source;

    private Observable(OnSubscribe<T> source) {
        this.source = source;
    }

    public static <T> Observable<T> create(OnSubscribe<T> source) {
        return new Observable<>(source);
    }

    public Disposable subscribe(Observer<? super T> observer) {
        AtomicBoolean disposed = new AtomicBoolean(false);

        Observer<T> safeObserver = new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (!disposed.get()) {
                    observer.onNext(item);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (!disposed.getAndSet(true)) {
                    observer.onError(t);
                }
            }

            @Override
            public void onComplete() {
                if (!disposed.getAndSet(true)) {
                    observer.onComplete();
                }
            }
        };

        try {
            source.subscribe(safeObserver);
        } catch (Throwable t) {
            safeObserver.onError(t);
        }

        return new Disposable() {
            @Override
            public void dispose() {
                disposed.set(true);
            }

            @Override
            public boolean isDisposed() {
                return disposed.get();
            }
        };
    }

    // ---------- Операторы ----------

    public <R> Observable<R> map(Function<? super T, ? extends R> mapper) {
        return new Observable<>(downstream ->
                Observable.this.subscribe(new Observer<T>() {
                    @Override
                    public void onNext(T item) {
                        try {
                            R mapped = mapper.apply(item);
                            downstream.onNext(mapped);
                        } catch (Throwable t) {
                            downstream.onError(t);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        downstream.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        downstream.onComplete();
                    }
                })
        );
    }

    public Observable<T> filter(Predicate<? super T> predicate) {
        return new Observable<>(downstream ->
                Observable.this.subscribe(new Observer<T>() {
                    @Override
                    public void onNext(T item) {
                        try {
                            if (predicate.test(item)) {
                                downstream.onNext(item);
                            }
                        } catch (Throwable t) {
                            downstream.onError(t);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        downstream.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        downstream.onComplete();
                    }
                })
        );
    }

    public <R> Observable<R> flatMap(
            Function<? super T, Observable<? extends R>> mapper) {

        return new Observable<>(downstream ->
                Observable.this.subscribe(new Observer<T>() {
                    @Override
                    public void onNext(T item) {
                        Observable<? extends R> inner;
                        try {
                            inner = mapper.apply(item);
                        } catch (Throwable t) {
                            downstream.onError(t);
                            return;
                        }

                        inner.subscribe(new Observer<R>() {
                            @Override
                            public void onNext(R r) {
                                downstream.onNext(r);
                            }

                            @Override
                            public void onError(Throwable t) {
                                downstream.onError(t);
                            }

                            @Override
                            public void onComplete() {
                                // завершение внутреннего потока не завершает общий
                            }
                        });
                    }

                    @Override
                    public void onError(Throwable t) {
                        downstream.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        downstream.onComplete();
                    }
                })
        );
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        return new Observable<>(downstream ->
                scheduler.execute(() -> Observable.this.source.subscribe(downstream))
        );
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        return new Observable<>(downstream ->
                Observable.this.subscribe(new Observer<T>() {
                    @Override
                    public void onNext(T item) {
                        scheduler.execute(() -> downstream.onNext(item));
                    }

                    @Override
                    public void onError(Throwable t) {
                        scheduler.execute(() -> downstream.onError(t));
                    }

                    @Override
                    public void onComplete() {
                        scheduler.execute(downstream::onComplete);
                    }
                })
        );
    }
}