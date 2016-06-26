/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels.refresh;

import edu.teco.ecqels.op.ECQELSOpVisitorBase;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.TransformBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.engine.main.StageGeneratorGeneric;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import edu.teco.ecqels.lang.op.OpRefreshableGraph;
import edu.teco.ecqels.lang.op.OpRefreshableService;
import edu.teco.ecqels.lang.op.OpRefreshableStream;
import edu.teco.ecqels.lang.op.OpStream;
import edu.teco.ecqels.lang.window.Duration;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class UpdateRefreshIntervalsTransform extends TransformBase {

    private final Op op;
    private final Map<Op, Duration> indirectRefreshIntervals = new HashMap<>();

    public UpdateRefreshIntervalsTransform(Op op) {
        this.op = op;
        findIndirectRefreshIntervals();
    }

    private void findIndirectRefreshIntervals() {
        // Find refreshable streams
        final List<OpRefreshableStream> refreshableStreams = new ArrayList<>();
        final Map<Triple, Op> triplesMapping = new HashMap<>();
        OpWalker.walk(op, new ECQELSOpVisitorBase() {

            @Override
            public void visit(OpRefreshableStream opStream) {
                refreshableStreams.add((OpRefreshableStream) opStream);
            }

            @Override
            public void visit(final OpGraph opGraph) {
                OpWalker.walk(opGraph, new OpVisitorBase() {
                    @Override
                    public void visit(OpTriple opTriple) {
                        triplesMapping.put(opTriple.getTriple(), opGraph);
                    }

                    @Override
                    public void visit(OpBGP opBGP) {
                        for (Triple triple : opBGP.getPattern().getList()) {
                            triplesMapping.put(triple, opGraph);
                        }
                    }
                });
            }

            @Override
            public void visit(final OpService opService) {
                OpWalker.walk(opService, new OpVisitorBase() {
                    @Override
                    public void visit(OpTriple opTriple) {
                        triplesMapping.put(opTriple.getTriple(), opService);
                    }

                    @Override
                    public void visit(OpBGP opBGP) {
                        for (Triple triple : opBGP.getPattern().getList()) {
                            triplesMapping.put(triple, opService);
                        }
                    }
                });
            }
        });
        if (!refreshableStreams.isEmpty()) {
            for (OpRefreshableStream refreshableStream : refreshableStreams) {
                Set<Op> dependentRefreshables = findDependendRefreshables(triplesMapping, refreshableStream.getOp().getNode());
                for (Op op : dependentRefreshables) {
                    indirectRefreshIntervals.put(op, (indirectRefreshIntervals.containsKey(op) && indirectRefreshIntervals.get(op).inNanoSec() < refreshableStream.getDuration().inNanoSec())
                            ? indirectRefreshIntervals.get(op)
                            : refreshableStream.getDuration());
                }
            }
        }
    }

    private Set<Op> findDependendRefreshables(Map<Triple, Op> triplesMapping, Node nodeOfInterest) {
        Set<Op> foundDependencies = new HashSet<Op>();
        List<Node> foundNodes = new ArrayList<Node>();
        findDependentTriplesRecursive(triplesMapping, foundDependencies, foundNodes, nodeOfInterest);
        return foundDependencies;
    }

    private void findDependentTriplesRecursive(Map<Triple, Op> triplesMapping, Set<Op> foundDependencies, List<Node> foundNodes, Node nodeOfInterest) {
        Map<Node, List<Op>> newlyFoundNodes = new HashMap<Node, List<Op>>();
        Iterator<Map.Entry<Triple, Op>> iter = triplesMapping.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Triple, Op> entry = iter.next();
            Triple triple = entry.getKey();
            if (triple.subjectMatches(nodeOfInterest)
                    || triple.predicateMatches(nodeOfInterest)
                    || triple.objectMatches(nodeOfInterest)) {
                if (triple.getSubject().isVariable() && !triple.getSubject().equals(nodeOfInterest) && !foundNodes.contains(triple.getSubject())) {
                    if (!newlyFoundNodes.containsKey(triple.getSubject())) {
                        newlyFoundNodes.put(triple.getSubject(), new ArrayList<Op>());
                    }
                    newlyFoundNodes.get(triple.getSubject()).add(entry.getValue());
                }
                if (triple.getPredicate().isVariable() && !triple.getPredicate().equals(nodeOfInterest) && !foundNodes.contains(triple.getPredicate())) {
                    if (!newlyFoundNodes.containsKey(triple.getPredicate())) {
                        newlyFoundNodes.put(triple.getPredicate(), new ArrayList<Op>());
                    }
                    newlyFoundNodes.get(triple.getPredicate()).add(entry.getValue());
                }
                if (triple.getObject().isVariable() && !triple.getObject().equals(nodeOfInterest) && !foundNodes.contains(triple.getObject())) {
                    if (!newlyFoundNodes.containsKey(triple.getObject())) {
                        newlyFoundNodes.put(triple.getObject(), new ArrayList<Op>());
                    }
                    newlyFoundNodes.get(triple.getObject()).add(entry.getValue());
                } else {
                    foundDependencies.add(entry.getValue());
                }
                iter.remove();
            }

        }
        for (Map.Entry<Node, List<Op>> entry : newlyFoundNodes.entrySet()) {
            foundNodes.add(entry.getKey());
            foundDependencies.addAll(entry.getValue());
            findDependentTriplesRecursive(triplesMapping, foundDependencies, foundNodes, entry.getKey());
        }
    }

    @Override
    public Op transform(OpExt opExt) {
        if (opExt instanceof OpRefreshableGraph) {
            return transform((OpRefreshableGraph) opExt);
        } else if (opExt instanceof OpRefreshableService) {
            return transform((OpRefreshableService) opExt);
        }
        return super.transform(opExt);
    }

    protected Op transform(OpRefreshableGraph opGraph) {
        OpRefreshableGraph result = opGraph;
        if (indirectRefreshIntervals.containsKey(opGraph)) {
            result = new OpRefreshableGraph(opGraph.getOp(),
                    (opGraph instanceof OpRefreshableGraph) && (((OpRefreshableGraph) opGraph).getDuration().inNanoSec() < indirectRefreshIntervals.get(opGraph).inNanoSec())
                    ? ((OpRefreshableGraph) opGraph).getDuration()
                    : indirectRefreshIntervals.get(opGraph));           
        }
        return result;
    }

    protected Op transform(OpRefreshableService opService) {
        OpRefreshableService result = opService;
        if (indirectRefreshIntervals.containsKey(opService)) {
            result = new OpRefreshableService(opService.getOp(),
                   (opService instanceof OpRefreshableService) && (((OpRefreshableService) opService).getDuration().inNanoSec() < indirectRefreshIntervals.get(opService).inNanoSec())
                    ? ((OpRefreshableService) opService).getDuration()
                    : indirectRefreshIntervals.get(opService));
        }
        return result;
    }

    @Override
    public Op transform(OpGraph opGraph, Op subOp) {
        Op result = opGraph;
        if (indirectRefreshIntervals.containsKey(opGraph)) {
            result = new OpRefreshableGraph(opGraph.getNode(), opGraph.getSubOp(),
                    indirectRefreshIntervals.get(opGraph));
        }
        return result;
    }

    @Override
    public Op transform(OpService opService, Op subOp) {
        Op result = opService;
        if (indirectRefreshIntervals.containsKey(opService)) {
            result = new OpRefreshableService(opService.getService(), opService.getSubOp(), null, opService.getSilent(),
                    indirectRefreshIntervals.get(opService));
        }
        return result;
    }
}
