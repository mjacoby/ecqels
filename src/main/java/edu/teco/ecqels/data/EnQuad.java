package edu.teco.ecqels.data;

import com.hp.hpl.jena.graph.Node;

public class EnQuad {

    private long gID;
    private Node subject;
    private Node prdeicate;
    private Node object;
    private long time;

    public EnQuad(long gID, Node subject, Node predicate, Node object) {
        this.gID = gID;
        this.subject = subject;
        this.prdeicate = predicate;
        this.object = object;
        time = System.nanoTime();
    }

    public long getGID() {
        return gID;
    }    

    public Node getSubject() {
        return subject;
    }

    public Node getPrdeicate() {
        return prdeicate;
    }

    public Node getObject() {
        return object;
    }

    public long getTime() {
        return time;
    }
}
