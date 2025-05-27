/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tomcat.util.threads;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.ExceptionUtils;
import org.apache.tomcat.util.net.SocketProcessorBase;
import org.apache.tomcat.util.net.SocketWrapperBase;
import org.apache.tomcat.util.res.StringManager;

import java.io.Serial;
import java.lang.reflect.Field;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * As task queue specifically designed to run with a thread pool executor. The
 * task queue is optimised to properly utilize threads within a thread pool
 * executor. If you use a normal queue, the executor will spawn threads when
 * there are idle threads and you won't be able to force items onto the queue
 * itself.
 */
public class TaskPriorityQueue extends PriorityBlockingQueue<Runnable> {

    @Serial
    private static final long serialVersionUID = 1L;
    protected static final StringManager sm = StringManager.getManager(TaskPriorityQueue.class);

    private transient volatile ThreadPoolExecutor parent = null;

    private static final Log log = LogFactory.getLog(TaskPriorityQueue.class);

    /**
     * Default array capacity. This is the same as the default for Java's PriorityBlockingQueue.
     */
    private static final int DEFAULT_INITIAL_CAPACITY = 11;

    /**
     * The maximum capacity of the queue.
     */
    private final int maxCapacity;

    public TaskPriorityQueue(int maxCapacity) {
        super(DEFAULT_INITIAL_CAPACITY, (a, b) -> {
            try {
                if (a instanceof SocketProcessorBase<?> && b instanceof SocketProcessorBase<?>) {
                    SocketWrapperBase<?> socketWrapperA = getSocketWrapper((SocketProcessorBase<?>) a);
                    SocketWrapperBase<?> socketWrapperB = getSocketWrapper((SocketProcessorBase<?>) b);

                    return Integer.compare(
                        socketWrapperA.getLocalPort(),
                        socketWrapperB.getLocalPort()
                    );
                }
            } catch (IllegalStateException e) {
                ExceptionUtils.handleThrowable(e);
            }

            log.error("Couldn't infer priority for tasks: " + a.getClass().getName() + " and " + b.getClass().getName());
            return 0; // Default comparison if not SocketWrapperBase<?>
        });
        this.maxCapacity = maxCapacity;
    }

    public void setParent(ThreadPoolExecutor tp) {
        parent = tp;
    }


    @Override
    public boolean offer(Runnable o) {
        //we are exceeding number of tasks in the queue, so we can't add more
        if (size() >= maxCapacity) {
            return false;
        }
        //we can't do any checks
        if (parent == null) {
            return super.offer(o);
        }
        //we are maxed out on threads, simply queue the object
        if (parent.getPoolSizeNoLock() == parent.getMaximumPoolSize()) {
            return super.offer(o);
        }
        //we have idle threads, just add it to the queue
        if (parent.getSubmittedCount() <= parent.getPoolSizeNoLock()) {
            return super.offer(o);
        }
        //if we have less threads than maximum force creation of a new thread
        if (parent.getPoolSizeNoLock() < parent.getMaximumPoolSize()) {
            return false;
        }
        //if we reached here, we need to add it to the queue
        return super.offer(o);
    }


    @Override
    public Runnable poll(long timeout, TimeUnit unit)
        throws InterruptedException {
        Runnable runnable = super.poll(timeout, unit);
        if (runnable == null && parent != null) {
            // the poll timed out, it gives an opportunity to stop the current
            // thread if needed to avoid memory leaks.
            parent.stopCurrentThreadIfNeeded();
        }
        return runnable;
    }

    @Override
    public Runnable take() throws InterruptedException {
        if (parent != null && parent.currentThreadShouldBeStopped()) {
            return poll(parent.getKeepAliveTime(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            // yes, this may return null (in case of timeout) which normally
            // does not occur with take()
            // but the ThreadPoolExecutor implementation allows this
        }
        return super.take();
    }

    private static SocketWrapperBase<?> getSocketWrapper(SocketProcessorBase<?> r) {
        try {
            Field f = r.getClass().getSuperclass().getDeclaredField("socketWrapper");
            f.setAccessible(true);
            return (SocketWrapperBase<?>) f.get(r);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IllegalStateException("Couldn't access socketWrapper", e);
        }
    }
}
