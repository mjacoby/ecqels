package edu.teco.ecqels.util;

import com.hp.hpl.jena.graph.Node;
import java.util.ArrayList;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorByType;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.OpWalker.WalkerVisitor;
import com.hp.hpl.jena.sparql.algebra.op.Op0;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.Op2;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpN;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import edu.teco.ecqels.lang.op.OpFilterWithHistory;
import edu.teco.ecqels.lang.op.OpRefreshableGraph;
import edu.teco.ecqels.lang.op.OpRefreshableService;
import edu.teco.ecqels.lang.op.OpStream;
import java.util.List;

public class Utils {

    
    public static Node toNode(String st) {
        return Node.createURI(st);
    }
    
    public static <R, T extends R> List<R> findInstacesOf(Op op, Class<T> type) {
        class CheckContainsInstanceOfOpVisitor extends OpVisitorByType {

            private final Class<T> type;
            public List<R> result = new ArrayList<>();

            public CheckContainsInstanceOfOpVisitor(Class<T> type) {
                this.type = type;
            }

            @Override
            protected void visitN(OpN op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visit2(Op2 op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visit1(Op1 op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visit0(Op0 op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visitExt(OpExt op) {
                checkIfIsInstanceOf(op);
            }

            private void checkIfIsInstanceOf(Op op) {
                if (type.isAssignableFrom(op.getClass())) {
                    result.add((T) op);
                }
            }
        }
        CheckContainsInstanceOfOpVisitor visitor = new CheckContainsInstanceOfOpVisitor(type);
        op.visit(new WalkerVisitor(visitor) {
            @Override
            protected void visitExt(OpExt op) {
                if (op instanceof OpStream) {
                    ((OpStream)op).getSubOp().visit(this);
                }
                if (op instanceof OpRefreshableGraph) {
                    ((OpRefreshableGraph)op).getOp().visit(this);
                }
                if (op instanceof OpRefreshableService) {
                    ((OpRefreshableService)op).getOp().visit(this);
                }
                if (op instanceof OpFilterWithHistory) {
                    ((OpFilterWithHistory)op).getOp().visit(this);
                }
                super.visitExt(op);
            }
        });
        return visitor.result;
    }
    
    public static boolean isSubOp(final Op subOp, final Op parent) {        
        class OpVisitorFindOp extends OpVisitorByType {

            public boolean containsOp = false;
            
            @Override
            protected void visitN(OpN op) {
                if (op.equals(subOp)) containsOp = true;
            }

            @Override
            protected void visit2(Op2 op) {
                if (op.equals(subOp)) containsOp = true;
            }

            @Override
            protected void visit1(Op1 op) {
                if (op.equals(subOp)) containsOp = true;
            }

            @Override
            protected void visit0(Op0 op) {
                if (op.equals(subOp)) containsOp = true;
            }

            @Override
            protected void visitExt(OpExt op) {
                if (op.equals(subOp)) containsOp = true;
            }
        }
        OpVisitorFindOp visitor = new OpVisitorFindOp();
        OpWalker.walk(parent, visitor);
        return visitor.containsOp;
    }

    public static boolean checkContainsInstacesOf(Op op, Class... classes) {
        class CheckContainsInstanceOfOpVisitor extends OpVisitorByType {

            public boolean containsInstanceOf = false;
            private final Class[] classes;

            public CheckContainsInstanceOfOpVisitor(Class[] classes) {
                this.classes = classes;
            }

            @Override
            protected void visitN(OpN op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visit2(Op2 op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visit1(Op1 op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visit0(Op0 op) {
                checkIfIsInstanceOf(op);
            }

            @Override
            protected void visitExt(OpExt op) {
                checkIfIsInstanceOf(op);
            }

            private void checkIfIsInstanceOf(Op op) {
                for (Class clazz : classes) {
                    if (clazz.isInstance(op)) {
                        containsInstanceOf = true;
                    }
                }
            }
        }
        CheckContainsInstanceOfOpVisitor visitor = new CheckContainsInstanceOfOpVisitor(classes);
        OpWalker.walk(op, visitor);
        return visitor.containsInstanceOf;
    }

    public static ArrayList<Var> quad2Vars(Quad quad) {
        ArrayList<Var> vars = new ArrayList<Var>();
        if (quad.getGraph().isVariable()) {
            vars.add((Var) quad.getGraph());
        }
        if (quad.getSubject().isVariable()) {
            vars.add((Var) quad.getSubject());
        }
        if (quad.getPredicate().isVariable()) {
            vars.add((Var) quad.getPredicate());
        }
        if (quad.getObject().isVariable()) {
            vars.add((Var) quad.getObject());
        }
        return vars;
    }
}
