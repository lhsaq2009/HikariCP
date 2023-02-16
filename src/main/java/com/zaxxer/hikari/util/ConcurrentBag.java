/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zaxxer.hikari.util;

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.elapsedNanos;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.*;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * This is a specialized concurrent bag that achieves superior performance
 * to LinkedBlockingQueue and LinkedTransferQueue for the purposes of a
 * connection pool.  It uses ThreadLocal storage when possible to avoid
 * locks, but resorts to scanning a common collection if there are no
 * available items in the ThreadLocal list.  Not-in-use items in the
 * ThreadLocal lists can be "stolen" when the borrowing thread has none
 * of its own.  It is a "lock-less" implementation using a specialized
 * AbstractQueuedLongSynchronizer to manage cross-thread signaling.
 * <p>
 * Note that items that are "borrowed" from the bag are not actually
 * removed from any collection, so garbage collection will not occur
 * even if the reference is abandoned.  Thus care must be taken to
 * "requite" borrowed objects otherwise a memory leak will result.  Only
 * the "remove" method can completely remove an object from the bag.
 * <p>
 * ConcurrentBag 是为追求链接池操作高性能而设计的并发工具。
 * 它使用 ThreadLocal 缓存来避免锁争抢，当 ThreadLocal 中没有可用的链接时会去公共集合中“借用”链接。
 * ThreadLocal 中处于 Not-in-use 状态的链接也可能会“借走”。
 * ConcurrentBag 使用 AbstractQueuedLongSynchronizer 来管理跨线程通信（实际新版本已经删掉了 AbstractQueuedLongSynchronizer ）。
 * <p>
 * 注意被“借走”的链接并没有从任何集合中删除，所以即使链接的引用被弃用也不会进行 gc。
 * 所以要及时将被“借走”的链接归还回来，否则可能会发生内存泄露。只有 remove 方法才会真正将链接从 ConcurrentBag 中删除。
 *
 * @param <T> the templated type to store in the bag
 * @author Brett Wooldridge
 * <p>
 * ConcurrentBag 是 HikariCP 中实现的一个无锁化集合，比 JDK 中的 LinkedBlockingQueue 和 LinkedTransferQueue 的性能更好。
 */
public class ConcurrentBag<T extends IConcurrentBagEntry> implements AutoCloseable {        // 一个 lock-free 集合
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentBag.class);

    private final CopyOnWriteArrayList<T> sharedList;           // 负责存放全部用于出借的资源
    private final boolean weakThreadLocals;                     // 是否使用弱引用

    private final ThreadLocal<List<Object>> threadList;         // 线程本地缓存，用于加速线程本地化资源访问
    private final IBagStateListener listener;                   // 添加元素的监听器，在 HikariPool 中实现
    private final AtomicInteger waiters;                        // 当前等待获取元素的线程数
    private volatile boolean closed;                            // ConcurrentBag 是否处于关于状态

    private final SynchronousQueue<T> handoffQueue;             // 接力队列，用于存在资源等待线程时的第一手资源交接

    public interface IConcurrentBagEntry {
        // 定义链接的状态
        int STATE_NOT_IN_USE = 0;                               // 未使用。可以被借走
        int STATE_IN_USE = 1;                                   // 正在使用。
        int STATE_REMOVED = -1;                                 // 被移除，只有调用 remove() 时会 CAS 改变为这个状态，修改成功后会从容器中被移除。
        int STATE_RESERVED = -2;                                // 被保留，不能被使用。往往是移除前执行保留操作。


        boolean compareAndSet(int expectState, int newState);   // CAS 对链接状态的操作

        void setState(int newState);

        int getState();
    }

    public interface IBagStateListener {
        void addBagItem(int waiting);
    }

    /**
     * Construct a ConcurrentBag with the specified listener.
     *
     * @param listener the IBagStateListener to attach to this bag
     */
    public ConcurrentBag(final IBagStateListener listener) {
        this.listener = listener;
        this.weakThreadLocals = useWeakThreadLocals();

        this.handoffQueue = new SynchronousQueue<>(true);       // 初始化，略
        this.waiters = new AtomicInteger();
        this.sharedList = new CopyOnWriteArrayList<>();
        if (weakThreadLocals) {
            this.threadList = ThreadLocal.withInitial(() -> new ArrayList<>(16));
        } else {
            this.threadList = ThreadLocal.withInitial(() -> new FastList<>(IConcurrentBagEntry.class, 16));     //
        }
    }

    /**
     * 2、获取链接
     * <p>
     * 链接获取顺序：
     * 1. 从线程本地缓存 ThreadList 中获取，这里保持的是该线程之前使用过的链接
     * 2. 从共享集合 sharedList 中获取，如果获取不到，会通知 listener 新建链接（但不一定真的会新建链接出来）
     * 3. 从 handoffQueue 中阻塞获取，新建的链接 或 一些转为可用的链接会放入该队列中
     * <p>
     * The method will borrow a BagEntry from the bag, blocking for the
     * specified timeout if none are available.
     *
     * @param timeout  how long to wait before giving up, in units of unit
     * @param timeUnit a <code>TimeUnit</code> determining how to interpret the timeout parameter
     * @return a borrowed instance from the bag or null if a timeout occurs
     * @throws InterruptedException if interrupted while waiting
     */
    public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException {
        // 01、先看是否能从 ThreadList 中拿到可用链接，这里的 List 通常为 FastList ；Try the thread-local list first
        final List<Object> list = threadList.get();
        for (int i = list.size() - 1; i >= 0; i--) {                // TODO：倒序获取，为什么从尾部？？
            final Object entry = list.remove(i);             // 从尾部好处不需要移动底层数组
            // ThreadLocal Key 一定是弱引用；获取链接，「连接」可能使用了弱引用
            @SuppressWarnings("unchecked") final T bagEntry = weakThreadLocals ? ((WeakReference<T>) entry).get() : (T) entry;
            // 如果能够获取「连接」且可用：STATE_NOT_IN_USE => STATE_IN_USE
            if (bagEntry != null && bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                return bagEntry;
            }
        }

        // 2. 如果 ThreadList 中没有可用的链接，则尝试从「共享集合」中获取链接；Otherwise, scan the shared list ... then poll the handoff queue
        final int waiting = waiters.incrementAndGet();                      //
        try {
            for (T bagEntry : sharedList) {
                if (bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                    // If we may have stolen another waiter's connection, request another bag add.
                    if (waiting > 1) {                                      // TODO：考虑公平 ？？？
                        // why? 这里可能把其它线程，提交的 addBagIte() 任务结果 ( poolEntry ) 给窃取了 ..
                        listener.addBagItem(waiting - 1);           // 通知监听器添加链接
                    }
                    return bagEntry;
                }
            }

            /**
             * {@link com.zaxxer.hikari.pool.HikariPool.PoolEntryCreator#call}
             * =>> {@link com.zaxxer.hikari.pool.HikariPool#createPoolEntry}
             */
            listener.addBagItem(waiting);                                   // =>> 考虑现有任务数量，可能会提交新任务 ( 添加 PoolEntry )

            // 3. 尝试从 handoffQueue 队列中获取。在等待时可能链接被新建 或 改为转为可用状态
            // SynchronousQueue 是一种无容量的 BlockingQueue，在 poll 时如果没有元素，则阻塞等待 timeout 时间
            timeout = timeUnit.toNanos(timeout);
            do {
                final long start = currentTime();
                final T bagEntry = handoffQueue.poll(timeout, NANOSECONDS);     // 等 new add 传递过来，
                if (bagEntry == null || bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
                    return bagEntry;
                }

                timeout -= elapsedNanos(start);
            } while (timeout > 10_000);

            return null;
        } finally {
            waiters.decrementAndGet();
        }
    }

    /**
     * 3、归还「连接」
     * <p>
     * 归还「连接」的顺序：
     *      1. 将「连接」置为可用状态 STATE_NOT_IN_USE
     *
     *      2. 如果有等待「连接」的线程，则将该「连接」通过 handoffQueue.offer(bagEntry) 给出去
     *         由于该「连接」可能在当前线程的 threadList 里，所以可以发现 A 线程的 threadList 中的「连接」可能被 B 线程使用
     *
     *      3. 将它放入当前线程的 theadList 中，
     *         这里可以看出来 threadList 一开始是空的，当线程从 sharedList 中借用了「连接」并使用完后，会放入自己的缓存中
     * <p>
     *
     * This method will return a borrowed object to the bag.  Objects
     * that are borrowed from the bag but never "requited" will result
     * in a memory leak.
     *
     * @param bagEntry the value to return to the bag
     * @throws NullPointerException  if value is null
     * @throws IllegalStateException if the bagEntry was not borrowed from the bag
     */
    public void requite(final T bagEntry) {     // 归还「连接」

        // 1. 将「连接」状态改为 STATE_NOT_IN_USE
        bagEntry.setState(STATE_NOT_IN_USE);

        // 2. 如果有等待「连接」的线程，将该「连接」交出去
        for (int i = 0; waiters.get() > 0; i++) {
            if (bagEntry.getState() != STATE_NOT_IN_USE || handoffQueue.offer(bagEntry)) {  // 归还链接时，接力：提供；非阻塞立即返回：true/false
                return;                                                                        // 若交出去，则直 return 了
            } else if ((i & 0xff) == 0xff) {                                                   // 如果循环了 255 次，把当前线程挂起一会
                parkNanos(MICROSECONDS.toNanos(10));
            } else {
                Thread.yield();
            }
        }

        // 3. TODO：将连接放入线程本地缓存 ThreadList 中；目的是是访问快 ？？      如果没有等待者或者循环了一圈没能放入交接队列，则放入 ThreadLocal
        final List<Object> threadLocalList = threadList.get();      // 写入
        if (threadLocalList.size() < 50) {
            threadLocalList.add(weakThreadLocals ? new WeakReference<>(bagEntry) : bagEntry);
        }
    }

    /**
     * 1、增加链接
     * <p>
     * 将新的链接放入 sharedList 中，如果有等待链接的线程，则将链接给该线程。
     * <p>
     * Add a new object to the bag for others to borrow.
     *
     * @param bagEntry an object to add to the bag
     */
    public void add(final T bagEntry) {
        if (closed) {
            LOGGER.info("ConcurrentBag has been closed, ignoring add()");
            throw new IllegalStateException("ConcurrentBag has been closed, ignoring add()");
        }

        sharedList.add(bagEntry);
        // TODO：为什么要子自旋等待呢？？扔进 sharedList 就好呀. 2023-02-16
        // 等待直到没有 waiter 或有线程拿走它；spin until a thread takes it or none are waiting
        while (waiters.get() > 0 && bagEntry.getState() == STATE_NOT_IN_USE && !handoffQueue.offer(bagEntry)) {     // TODO：?? 为什么通过 容器传递 ？？
            Thread.yield();     // yield 什么都不做，只是为了让渡 CPU 使用，避免长期占用
        }
    }

    /**
     * Remove a value from the bag.  This method should only be called
     * with objects obtained by <code>borrow(long, TimeUnit)</code> or <code>reserve(T)</code>
     *
     * @param bagEntry the value to remove
     * @return true if the entry was removed, false otherwise
     * @throws IllegalStateException if an attempt is made to remove an object
     *                               from the bag that was not borrowed or reserved first
     */
    public boolean remove(final T bagEntry) {
        if (!bagEntry.compareAndSet(STATE_IN_USE, STATE_REMOVED) && !bagEntry.compareAndSet(STATE_RESERVED, STATE_REMOVED) && !closed) {
            LOGGER.warn("Attempt to remove an object from the bag that was not borrowed or reserved: {}", bagEntry);
            return false;
        }

        final boolean removed = sharedList.remove(bagEntry);
        if (!removed && !closed) {
            LOGGER.warn("Attempt to remove an object from the bag that does not exist: {}", bagEntry);
        }

        threadList.get().remove(bagEntry);

        return removed;
    }

    /**
     * Close the bag to further adds.
     */
    @Override
    public void close() {
        closed = true;
    }

    /**
     * This method provides a "snapshot" in time of the BagEntry
     * items in the bag in the specified state.  It does not "lock"
     * or reserve items in any way.  Call <code>reserve(T)</code>
     * on items in list before performing any action on them.
     *
     * @param state one of the {@link IConcurrentBagEntry} states
     * @return a possibly empty list of objects having the state specified
     */
    public List<T> values(final int state) {
        final List<T> list = sharedList.stream().filter(e -> e.getState() == state).collect(Collectors.toList());
        Collections.reverse(list);
        return list;
    }

    /**
     * This method provides a "snapshot" in time of the bag items.  It
     * does not "lock" or reserve items in any way.  Call <code>reserve(T)</code>
     * on items in the list, or understand the concurrency implications of
     * modifying items, before performing any action on them.
     *
     * @return a possibly empty list of (all) bag items
     */
    @SuppressWarnings("unchecked")
    public List<T> values() {
        return (List<T>) sharedList.clone();
    }

    /**
     * The method is used to make an item in the bag "unavailable" for
     * borrowing.  It is primarily used when wanting to operate on items
     * returned by the <code>values(int)</code> method.  Items that are
     * reserved can be removed from the bag via <code>remove(T)</code>
     * without the need to unreserve them.  Items that are not removed
     * from the bag can be make available for borrowing again by calling
     * the <code>unreserve(T)</code> method.
     *
     * @param bagEntry the item to reserve
     * @return true if the item was able to be reserved, false otherwise
     */
    public boolean reserve(final T bagEntry) {      // 状态标记；ConcurrentBag.unreserve
        return bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_RESERVED);
    }

    /**
     * This method is used to make an item reserved via <code>reserve(T)</code>
     * available again for borrowing.
     *
     * @param bagEntry the item to unreserve
     */
    @SuppressWarnings("SpellCheckingInspection")
    public void unreserve(final T bagEntry) {       // 源码里并无使用呀，
        if (bagEntry.compareAndSet(STATE_RESERVED, STATE_NOT_IN_USE)) {
            // spin until a thread takes it or none are waiting
            while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {        // 没有调用处呢，不关注这
                Thread.yield();
            }
        } else {
            LOGGER.warn("Attempt to relinquish an object to the bag that was not reserved: {}", bagEntry);
        }
    }

    /**
     * Get the number of threads pending (waiting) for an item from the
     * bag to become available.
     *
     * @return the number of threads waiting for items from the bag
     */
    public int getWaitingThreadCount() {
        return waiters.get();
    }

    /**
     * Get a count of the number of items in the specified state at the time of this call.
     *
     * @param state the state of the items to count
     * @return a count of how many items in the bag are in the specified state
     */
    public int getCount(final int state) {
        int count = 0;
        for (IConcurrentBagEntry e : sharedList) {
            if (e.getState() == state) {
                count++;
            }
        }
        return count;
    }

    public int[] getStateCounts() {
        final int[] states = new int[6];
        for (IConcurrentBagEntry e : sharedList) {
            ++states[e.getState()];
        }
        states[4] = sharedList.size();
        states[5] = waiters.get();

        return states;
    }

    /**
     * Get the total number of items in the bag.
     *
     * @return the number of items in the bag
     */
    public int size() {
        return sharedList.size();
    }

    public void dumpState() {
        sharedList.forEach(entry -> LOGGER.info(entry.toString()));
    }

    /**
     * Determine whether to use WeakReferences based on whether there is a
     * custom ClassLoader implementation sitting between this class and the
     * System ClassLoader.
     *
     * @return true if we should use WeakReferences in our ThreadLocals, false otherwise
     */
    private boolean useWeakThreadLocals() {
        try {
            if (System.getProperty("com.zaxxer.hikari.useWeakReferences") != null) {   // undocumented manual override of WeakReference behavior
                return Boolean.getBoolean("com.zaxxer.hikari.useWeakReferences");
            }

            return getClass().getClassLoader() != ClassLoader.getSystemClassLoader();
        } catch (SecurityException se) {
            return true;
        }
    }
}
