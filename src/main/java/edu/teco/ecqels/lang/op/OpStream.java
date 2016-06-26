package edu.teco.ecqels.lang.op;

import edu.teco.ecqels.window.Window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVars;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterConcat;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterNullIterator;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRoot;
import com.hp.hpl.jena.sparql.engine.main.QC;
import com.hp.hpl.jena.sparql.engine.main.VarFinder;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.sse.Tags;
import com.hp.hpl.jena.sparql.sse.writers.WriterLib;
import com.hp.hpl.jena.sparql.sse.writers.WriterOp;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.util.FmtUtils;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;
import edu.teco.ecqels.query.execution.QueryExecutionContext;
import edu.teco.ecqels.lang.window.WindowInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.openjena.atlas.io.IndentedWriter;

public class OpStream extends OpExt {

    protected final WindowInfo windowInfo;
    protected final Node node;
    protected final Op subOp;
    protected final BasicPattern pattern;

    public OpStream(Node node, Op subOp, BasicPattern pattern, WindowInfo windowInfo) {
        super("stream");
        if (node.isVariable() && !VarFinder.fixed(subOp).contains(node)) {
            throw new IllegalArgumentException("node '" + node + "' is variable but is not declared in suboperation '" + subOp + "'");
        }
        this.windowInfo = windowInfo;
        this.node = node;
        this.subOp = subOp;
        this.pattern = pattern;
    }

    public WindowInfo getWindowInfo() {
        return windowInfo;
    }

    public Node getNode() {
        return node;
    }

    public BasicPattern getPattern() {
        return pattern;
    }

    public Op getSubOp() {
        return subOp;
    }

    @Override
    public Op effectiveOp() {
        // TODO integrate pattern
        return OpJoin.create(new OpGraph(node, new OpBGP(pattern)), subOp);
    }

    @Override
    public QueryIterator eval(QueryIterator input, ExecutionContext execCxt) {
//        if (execCxt.getContext().isDefined(QueryExecutionContext.SYMBOL)) {
//            QueryExecutionContext queryContext = (QueryExecutionContext) execCxt.getContext().get(QueryExecutionContext.SYMBOL);
//            QueryIterConcat result = new QueryIterConcat(execCxt);
//            // execute on every stream            
//            for (Map.Entry<Node, Window> stream : queryContext.getCurrentStreamBindings().entrySet()) {
//                OpGraph op = new OpGraph(stream.getKey(), new OpBGP(pattern));
//                ExecutionContext context = new ExecutionContext(execCxt.getContext(), null, stream.getValue().getDatasetGraph(), execCxt.getExecutor());
//                //result.add(QC.execute(op, currentUris, context));
//                result.add(QC.execute(op, input, context));
//            }
//            return result;
//        }
//        return QC.execute(this, input, execCxt);
        return new QueryIterNullIterator(execCxt);
        //return QueryIterRoot.create(execCxt);
    }

    @Override
    public void outputArgs(IndentedWriter out, SerializationContext sCxt) {
        out.println(FmtUtils.stringForNode(node, sCxt) + "[" + windowInfo.toString() + "] {" + pattern.toString() + "}");
        out.ensureStartOfLine();        
        WriterOp.output(out, subOp, sCxt);
    }

    @Override
    public int hashCode() {
        return node.hashCode() ^ /*pattern.hashCode() ^*/ windowInfo.hashCode();
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        if (!(other instanceof OpStream)) {
            return false;
        }
        OpStream otherOp = (OpStream) other;
        return otherOp.node.equals(node) && otherOp.subOp.equalTo(subOp, labelMap) && otherOp.windowInfo.equals(windowInfo);
    }
}
