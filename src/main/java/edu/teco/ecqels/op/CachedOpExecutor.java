/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.op;

import edu.teco.ecqels.query.iterator.QueryIteratorCopy;
import edu.teco.ecqels.query.execution.QueryExecutionContext;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;

import com.hp.hpl.jena.sparql.engine.main.OpExecutor;
import static com.hp.hpl.jena.sparql.engine.main.OpExecutor.createRootQueryIterator;
import com.hp.hpl.jena.sparql.engine.main.OpExecutorFactory;
import com.hp.hpl.jena.tdb.solver.OpExecutorTDB;
import edu.teco.ecqels.lang.op.OpStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class CachedOpExecutor extends OpExecutorTDB {

    private static final Logger logger = LogManager.getLogger();

    public static final OpExecutorFactory CQELSOpExecFactory = new OpExecutorFactory() {
        @Override
        public OpExecutor create(ExecutionContext execCxt) {
            return new CachedOpExecutor(execCxt);
        }
    };

    private static OpExecutor createOpExecutor(ExecutionContext execCxt) {
        OpExecutorFactory factory = execCxt.getExecutor();
        if (factory == null) {
            factory = CQELSOpExecFactory;
        }
        if (factory == null) {
            return new CachedOpExecutor(execCxt);
        }
        return factory.create(execCxt);
    }

    static QueryIterator execute(Op op, ExecutionContext execCxt) {
        return execute(op, createRootQueryIterator(execCxt), execCxt);
    }

    static QueryIterator execute(Op op, QueryIterator qIter, ExecutionContext execCxt) {
        OpExecutor exec = createOpExecutor(execCxt);
        QueryIterator q = exec.executeOp(op, qIter);
        return q;
    }

    protected CachedOpExecutor(ExecutionContext execCxt) {
        super(execCxt);
    }

    @Override
    public QueryIterator executeOp(Op op, QueryIterator input) {
        long start = System.currentTimeMillis();
        QueryIterator resultToReturn = null;
        String executeType = "[unknown]";
        if (execCxt.getContext().isDefined(QueryExecutionContext.SYMBOL)) {
            QueryExecutionContext queryContext = (QueryExecutionContext) execCxt.getContext().get(QueryExecutionContext.SYMBOL);
            QueryIterator result = null;
            // force refresh occures with Stream, RefreshableStream, RefreshableGraph, RefreshableService
            if (queryContext.isForceRefresh(op)) {
                executeType = "[execute]";
                result = super.executeOp(op, input);
            } else if (queryContext.isRefreshed(op)) {
                executeType = "[cache]";
                input.close();
                result = queryContext.getCache().get(op);
            } else if (queryContext.isAffectedFromRefresh(op)) {
                executeType = "[execute]";
                result = super.executeOp(op, input);
                //if (queryContext.isCacheable(op)) {
                //    result = queryContext.getCache().put(op, result);
                //}
            } else if (queryContext.isCacheable(op)) {
                if (queryContext.getCache().containsKey(op)) {
                    executeType = "[cache]";
                    input.close();
                } else {
                    executeType = "[execute]";
                }
//                result = queryContext.getCache().put(op,
//                        queryContext.getCache().containsKey(op)
//                                ? queryContext.getCache().get(op)
//                                : super.executeOp(op, input));
                result = queryContext.getCache().containsKey(op)
                        ? queryContext.getCache().get(op)
                        : queryContext.getCache().put(op, super.executeOp(op, input));

            } else {
                if (op instanceof OpStream && queryContext.getCache().containsKey(op)) {
                    //always fetch streams from cache as they are resposnible theirselves to refresh themselves!
                    executeType = "[cache]";
                    result = queryContext.getCache().get(op);
                } else {
                    executeType = "[execute]";
                    result = super.executeOp(op, input);
                }
            }
            //resultToReturn = result;
            resultToReturn = printIntermediateResult(op, result);
        } else {
            resultToReturn = printIntermediateResult(op, super.executeOp(op, input));
        }
        if (logger.isDebugEnabled()) {
            logger.debug("execution type: " + executeType + "executing took " + (System.currentTimeMillis() - start) + "ms for op \n" + op);
        }
        return resultToReturn;
    }

    private QueryIterator printIntermediateResult(Op op, QueryIterator iterator) {
        // return iterator;
        QueryIteratorCopy copy;
        if (iterator instanceof QueryIteratorCopy) {
            copy = (QueryIteratorCopy) iterator;
        } else {
            copy = new QueryIteratorCopy(iterator, execCxt);
        }

        if (logger.isEnabled(Level.ALL)) {
            QueryIterator temp = copy.copy();
            logger.debug("result for op " + op);
            while (temp.hasNext()) {
                logger.debug("-  " + temp.next() + "\n");
            }
        }
        QueryIterator result = copy.copy();
        copy.close();
        return result;
    }
}
