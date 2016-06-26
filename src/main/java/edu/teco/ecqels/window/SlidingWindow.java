package edu.teco.ecqels.window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Quad;
import edu.teco.ecqels.Engine;
import edu.teco.ecqels.lang.window.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SlidingWindow extends ScheduledRefreshableWindow {

    protected Duration size;
    protected Duration slide;
    protected long windowStart;
    protected final ConcurrentMap<Long, Quad> cache;

    public SlidingWindow(Engine engine, Node streamNode, BasicPattern pattern, Duration size, Duration slide) {
        super(engine, streamNode, pattern);
        if (size == null || size.inMiliSec() <= 0) {
            throw new IllegalArgumentException("window size must be non-null and > 0");
        }
        if (slide == null || slide.inMiliSec() <= 0) {
            throw new IllegalArgumentException("window slide must be non-null and > 0");
        }
        this.size = size;
        this.slide = slide;
        //cache = new TreeMap<>();
        cache = new ConcurrentHashMap<>();
        windowStart = System.nanoTime();
    }

    @Override
    public void add(final Quad quad) {
        // only add if in curretn window
        long timestamp = System.nanoTime();
        //   if (timestamp <= windowStart + size.inNanoSec()) {
        cache.put(timestamp, quad);
        super.add(quad);
        // }
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public long getRefreshInterval() {
        return slide.inMiliSec();
    }

    @Override
    public void purgeBeforeExecution() {
        try {
            synchronized (this) {
                long minTimestamp = System.nanoTime() - size.inNanoSec();
                for (Map.Entry<Long, Quad> entry : cache.entrySet()) {
                    if (entry.getKey() < minTimestamp) {
                        cache.remove(entry.getKey());
                        datasetGraph.delete(entry.getValue());
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("error purging window" + e);
        }
//        SortedMap<Long, Quad> toRemove = cache.headMap(minTimestamp);
//        toRemove.keySet().forEach(key -> cache.remove(key));
//        toRemove.values().forEach(quad -> datasetGraph.delete(quad));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SlidingWindow)) {
            return false;
        }
        SlidingWindow window = (SlidingWindow) obj;
        return window.size.equals(size) && window.slide.equals(slide);
    }

    @Override
    public Window clone() {
        return new SlidingWindow(engine, streamNode, pattern, size, slide);
    }

}
