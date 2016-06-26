/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.refresh;

import edu.teco.ecqels.event.RefreshRequestedEvent;
import edu.teco.ecqels.event.RefreshRequestedListener;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import javax.swing.event.EventListenerList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class RefreshManager {

    private static final Logger logger = LogManager.getLogger();
    protected Map<Callable<RefreshRequest>, Long> tasks;
    protected Timer timer = new Timer();
    protected ScheduledExecutorService executor;
    protected EventListenerList listeners = new EventListenerList();

    public RefreshManager() {
        tasks = new HashMap<>();
    }

    public void schedule(Callable<RefreshRequest> task, long interval) {
        tasks.put(task, interval);
    }
    
    public void unschedule(Callable<RefreshRequest> task) {
        tasks.remove(task);
    }

    public void stop() {
        timer.cancel();
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    public void start() {
        if (tasks.isEmpty()) {
            return;
        }
        long gcd = gcd(tasks.values().toArray(new Long[tasks.size()]));
        long lcm = lcm(tasks.values().toArray(new Long[tasks.size()]));
        executor = Executors.newScheduledThreadPool(2);
        timer.schedule(new TimerTask() {
            long timeExpired = lcm;

            @Override
            public void run() {
                RefreshRequest finalRequest = new RefreshRequest();
                // find all tasks to execute here
                timeExpired = (timeExpired % gcd) + lcm;
                Set<Callable<RefreshRequest>> toRun = tasks.entrySet().stream().filter(entry -> (timeExpired % entry.getValue()) == 0).collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getKey())).keySet();
                CompletionService<RefreshRequest> completionService = new ExecutorCompletionService<>(executor);
                toRun.stream().forEach((action) -> {
                    completionService.submit(action);
                });
                int n = toRun.size();
                for (int i = 0; i < n; ++i) {
                    RefreshRequest request;
                    try {
                        request = completionService.take().get();
                        if (request != null) {
                            finalRequest.addSource(request);
                        }
                    } catch (InterruptedException | ExecutionException ex) {
                        logger.error(ex);
                    }
                }
                fireRefreshRequestedListener(new RefreshRequestedEvent(this, finalRequest));
            }
        }, lcm, lcm);
    }

    public void addRefreshRequestedListener(RefreshRequestedListener listener) {
        listeners.add(RefreshRequestedListener.class, listener);
    }

    public void removeRefreshRequestedListener(RefreshRequestedListener listener) {
        listeners.remove(RefreshRequestedListener.class, listener);
    }

    protected void fireRefreshRequestedListener(RefreshRequestedEvent e) {
        Object[] temp = listeners.getListenerList();
        for (int i = 0; i < temp.length; i = i + 2) {
            if (temp[i] == RefreshRequestedListener.class) {
                ((RefreshRequestedListener) temp[i + 1]).refreshRequested(e);
            }
        }
    }

    protected long gcd(long a, long b) {
        while (b > 0) {
            long temp = b;
            b = a % b; // % is remainder
            a = temp;
        }
        return a;
    }

    protected long gcd(Long[] input) {
        long result = input[0];
        for (int i = 1; i < input.length; i++) {
            result = gcd(result, input[i]);
        }
        return result;
    }

    protected long lcm(long a, long b) {
        return a * (b / gcd(a, b));
    }

    protected long lcm(Long[] input) {
        long result = input[0];
        for (int i = 1; i < input.length; i++) {
            result = lcm(result, input[i]);
        }
        return result;
    }
}
