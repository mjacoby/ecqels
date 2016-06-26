package edu.teco.ecqels.lang.op;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
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
public class OpRefreshableStream extends OpExt implements OpRefreshable {

    private final Duration duration;
    private final OpStream op;

    public OpRefreshableStream(OpStream op, Duration duration) {
        super("stream");
        this.duration = duration;
        this.op = op;
    }

    public Duration getDuration() {
        return duration;
    }
    
    public OpStream getOp() {
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
        out.println(FmtUtils.stringForNode(op.getNode(), sCxt)) ;
        out.ensureStartOfLine();
        WriterOp.output(out, new OpBGP(op.getPattern()), sCxt);
        WriterOp.output(out, op.getSubOp(), sCxt);
    }

    @Override
    public int hashCode() {
        return op.hashCode() ^ duration.hashCode();
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if (!(other instanceof OpRefreshableStream)) {
            return false;
        }
        OpRefreshableStream otherOp = (OpRefreshableStream) other;
        return otherOp.op.equalTo(op, labelMap) && otherOp.duration.equals(duration);
    }

}
