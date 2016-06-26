package edu.teco.ecqels.query.iterator;

import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Michael Jacoby
 * @organization Karlsruhe Institute of Technology, Germany www.kit.edu
 * @email michael.jacoby@student.kit.edu
 */
public class QueryIteratorCache<T> {

    private final Map<T, QueryIteratorCopy> cache = new ConcurrentHashMap<>();
    private final ExecutionContext context;
    
    public QueryIteratorCache(ExecutionContext context){
        this.context = context;
    }

    public boolean containsKey(T key) {
        return cache.containsKey(key);
    }

    public QueryIterator put(T key, QueryIterator value) {
        if (cache.containsKey(key)) {
            cache.get(key).close();
        }
        cache.put(key, new QueryIteratorCopy(value, context));
        return cache.get(key);
    }

    public QueryIterator get(T key) {
        if (containsKey(key)) {
            QueryIteratorCopy iterator = cache.get(key);
            return iterator.copy();
        }
        return null;
    }

    public QueryIterator remove(T key) {
        return cache.remove(key);
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    public int size() {
        return cache.size();
    }

    @Override
    public int hashCode() {
        return cache.hashCode();
    }

    public void flush() {
        cache.clear();
    }

    @Override
    public boolean equals(Object o) {
        return cache.equals(o);
    }
}
