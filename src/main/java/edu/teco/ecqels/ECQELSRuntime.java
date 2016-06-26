/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.teco.ecqels;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.core.Quad;
import edu.teco.ecqels.continuous.ContinuousConstruct;
import edu.teco.ecqels.continuous.ContinuousSelect;
import edu.teco.ecqels.stream.RDFStream;
import edu.teco.ecqels.stream.RunnableRDFStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class ECQELSRuntime {

    private final Engine engine;
    private final ExecutorService executorService;
    private final List<RDFStream> streams = new ArrayList<>();
    private boolean running = false;

    public ECQELSRuntime() {
        engine = new Engine();
        executorService = Executors.newCachedThreadPool();
    }

    public void registerStream(RDFStream stream, String metadataGraph, String metadata) {
        engine.addRDF(metadataGraph, metadata, "N-TRIPLES");
        addStream(stream);
    }

    public void unregisterStream(RDFStream stream, String metadataGraph, String metadata) {
        engine.deleteRDF(metadataGraph, metadata);
        if (streams.contains(stream)) {
            stream.stop();
            streams.remove(stream);
        }
    }

    public Engine getEngine() {
        return engine;
    }

    public void addStream(RDFStream stream) {
        streams.add(stream);
        if (running && stream instanceof RunnableRDFStream) {
            RunnableRDFStream runnable = (RunnableRDFStream) stream;
            if (!runnable.isRunning()) {
                executorService.execute(runnable);
            };
        }
    }

    public void addStreams(List<? extends RDFStream> streams) {
        for (RDFStream stream : streams) {
            addStream(stream);
        }
    }

    public void start() {
        for (RDFStream stream : streams) {
            if (stream instanceof RunnableRDFStream) {
                RunnableRDFStream runnable = (RunnableRDFStream) stream;
                if (!runnable.isRunning()) {
                    executorService.execute(runnable);
                }
            }
        }
        running = true;
    }

    public void shutdown() {
        stop();
        engine.shutdown();
    }

    public void stop() {
        running = false;
        streams.forEach((stream) -> {
            stream.stop();
        });
        try {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Logger.getLogger(Engine.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public ContinuousSelect registerSelect(String query) {
        return registerSelect(engine.parse(query));
    }

    public ContinuousSelect registerSelect(Query query) {
        return registerSelect(query, new HashMap<>());
    }

    public ContinuousSelect registerSelect(Query query, Map<String, String> variableBindings) {
        return engine.registerSelect(query, variableBindings);
    }

    public void unregisterSelect(ContinuousSelect query) {
        engine.unregisterSelect(query);
    }

    public void unregisterConstruct(ContinuousConstruct query) {
        engine.unregisterConstruct(query);
    }

    public ContinuousConstruct registerConstruct(String query) {
        return registerConstruct(engine.parse(query));
    }

    public ContinuousConstruct registerConstruct(Query query) {
        return registerConstruct(query, new HashMap<>());
    }

    public ContinuousConstruct registerConstruct(Query query, Map<String, String> variableBindings) {
        return engine.registerConstruct(query, variableBindings);
    }

    public void send(Quad quad) {
        send(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
    }

    public void send(Node graph, Triple triple) {
        send(graph, triple.getSubject(), triple.getPredicate(), triple.getObject());
    }

    public void send(Node graph, Node s, Node p, Node o) {
        engine.send(graph, s, p, o);
    }

    public void sendAsync(Quad quad) {
        sendAsync(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
    }

    public void sendAsync(Node graph, Triple triple) {
        sendAsync(graph, triple.getSubject(), triple.getPredicate(), triple.getObject());
    }

    public void sendAsync(Node graph, Node s, Node p, Node o) {
        engine.sendAsync(graph, s, p, o);
    }
}
