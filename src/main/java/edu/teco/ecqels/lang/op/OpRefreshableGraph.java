package edu.teco.ecqels.lang.op;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.main.QC;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.sse.writers.WriterOp;
import com.hp.hpl.jena.sparql.util.FmtUtils;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;
import edu.teco.ecqels.lang.window.Duration;
import org.openjena.atlas.io.IndentedWriter;

/**
 *
 * @author Michael Jacoby
 * @organization Karlsruhe Institute of Technology, Germany www.kit.edu
 * @email michael.jacoby@student.kit.edu
 */
public class OpRefreshableGraph extends OpExt implements OpRefreshable {

    private final Duration duration;
    private final OpGraph op;

    public OpRefreshableGraph(Node node, Op pattern, Duration duration) {
        this(new OpGraph(node, pattern), duration);
    }
    
    public OpRefreshableGraph(OpGraph op, Duration duration) {
        super("graph");
        this.op = op;
        this.duration = duration;
    }

    public Duration getDuration() {
        return duration;
    }
    
    public OpGraph getOp() {
        return op;
    }

    @Override
    public Op effectiveOp() {
        return op;
    }

    @Override
    public QueryIterator eval(QueryIterator input, ExecutionContext execCxt) {
        return QC.execute(op, input, execCxt);
    }

    @Override
    public void outputArgs(IndentedWriter out, SerializationContext sCxt) {
        out.print("[REFRESH " + duration + "]"); 
        WriterOp.output(out, op, sCxt);        
    }

    @Override
    public int hashCode() {
        return op.hashCode() ^ duration.hashCode();
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if (!(other instanceof OpRefreshableGraph)) {
            return false;
        }
        OpRefreshableGraph otherOp = (OpRefreshableGraph) other;
        return otherOp.op.equalTo(op, labelMap) && otherOp.duration.equals(duration);
    }

}
