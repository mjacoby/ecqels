package edu.teco.ecqels.algebra;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.sparql.ARQInternalErrorException;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpExtBuilder;
import com.hp.hpl.jena.sparql.algebra.OpExtRegistry;
import com.hp.hpl.jena.sparql.algebra.Table;
import com.hp.hpl.jena.sparql.algebra.TableFactory;
import com.hp.hpl.jena.sparql.algebra.Transform;
import com.hp.hpl.jena.sparql.algebra.Transformer;
import com.hp.hpl.jena.sparql.algebra.op.OpAssign;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpExtend;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpGroup;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLabel;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpList;
import com.hp.hpl.jena.sparql.algebra.op.OpMinus;
import com.hp.hpl.jena.sparql.algebra.op.OpNull;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformSimplify;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.PathBlock;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.E_Exists;
import com.hp.hpl.jena.sparql.expr.E_LogicalNot;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprLib;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.path.PathLib;
import com.hp.hpl.jena.sparql.sse.Item;
import com.hp.hpl.jena.sparql.sse.ItemList;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementAssign;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementData;
import com.hp.hpl.jena.sparql.syntax.ElementExists;
import com.hp.hpl.jena.sparql.syntax.ElementFetch;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementMinus;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.sparql.syntax.ElementNotExists;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementService;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.sparql.syntax.ElementUnion;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.sparql.util.Utils;
import edu.teco.ecqels.lang.element.ElementFilterWithHistory;
import edu.teco.ecqels.lang.element.ElementHistory;
import edu.teco.ecqels.lang.element.ElementRefreshableNamedGraph;
import edu.teco.ecqels.lang.element.ElementRefreshableService;
import edu.teco.ecqels.lang.element.ElementRefreshableStream;
import edu.teco.ecqels.lang.element.ElementStream;
import edu.teco.ecqels.lang.op.OpFilterWithHistory;
import edu.teco.ecqels.lang.op.OpHistory;
import edu.teco.ecqels.lang.op.OpRefreshableGraph;
import edu.teco.ecqels.lang.op.OpRefreshableService;
import edu.teco.ecqels.lang.op.OpRefreshableStream;
import edu.teco.ecqels.lang.op.OpStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import org.openjena.atlas.lib.Pair;
import org.openjena.atlas.logging.Log;

/**
 *
 * @author Michael Jacoby <michael.jacoby@student.kit.edu>
 */
public class AlgebraGenerator {

    // Fixed filter position means leave exactly where it is syntactically (illegal SPARQL)
    // Helpful only to write exactly what you mean and test the full query compiler.
    private boolean fixedFilterPosition = false;
    private Context context;
    private final int subQueryDepth;

    // simplifyInAlgebraGeneration=true is the alternative reading of
    // the DAWG Algebra translation algorithm. 
    // If we simplify during algebra generation, it changes the SPARQL for OPTIONAL {{ FILTER }}
    // The  {{}} results in (join unit (filter ...)) the filter is not moved
    // into the LeftJoin.  
    static final private boolean applySimplification = true;                   // False allows raw algebra to be generated (testing) 
    static final private boolean simplifyTooEarlyInAlgebraGeneration = false;  // False is the correct setting. 

    public AlgebraGenerator(Context context) {
        this(context != null ? context : ARQ.getContext().copy(), 0);
    }

    public AlgebraGenerator() {
        this(null);
    }

    private AlgebraGenerator(Context context, int depth) {
        this.context = context;
        this.subQueryDepth = depth;
    }

    //-- Public operations.  Do not call recursively (call compileElement).
    // These operations apply the simplification step which is done, once, at the end.
    public Op compile(Query query) {
        Op op = compile(query.getQueryPattern());     // Not compileElement - may need to apply simplification.

        op = compileModifiers(query, op);
        return op;
    }

    protected static Transform simplify = new TransformSimplify();

    // Compile any structural element
    public Op compile(Element elt) {
        Op op = compileElement(elt);
        Op op2 = op;
        if (!simplifyTooEarlyInAlgebraGeneration && applySimplification && simplify != null) {
            op2 = simplify(op);
        }
        return op2;
    }

    private static Op simplify(Op op) {
        return Transformer.transform(simplify, op);
    }

    // This is the operation to call for recursive application.
    protected Op compileElement(Element elt) {
        if (elt instanceof ElementRefreshableNamedGraph) {
            return compileElementRefreshableNamedGraph((ElementRefreshableNamedGraph) elt);
        }
        if (elt instanceof ElementRefreshableService) {
            return compileElementRefreshableService((ElementRefreshableService) elt);
        }
        if (elt instanceof ElementRefreshableStream) {
            return compileElementRefreshableStream((ElementRefreshableStream) elt);
        }
        if (elt instanceof ElementStream) {
            return compileElementStream((ElementStream) elt);
        }
        if (elt instanceof ElementFilterWithHistory) {
            return compileElementFilterWithHistory((ElementFilterWithHistory) elt);
        }
        if (elt instanceof ElementGroup) {
            return compileElementGroup((ElementGroup) elt);
        }

        if (elt instanceof ElementUnion) {
            return compileElementUnion((ElementUnion) elt);
        }

        if (elt instanceof ElementNamedGraph) {
            return compileElementGraph((ElementNamedGraph) elt);
        }

        if (elt instanceof ElementService) {
            return compileElementService((ElementService) elt);
        }

        if (elt instanceof ElementFetch) {
            return compileElementFetch((ElementFetch) elt);
        }

        // This is only here for queries built programmatically
        // (triple patterns not in a group) 
        if (elt instanceof ElementTriplesBlock) {
            return compileBasicPattern(((ElementTriplesBlock) elt).getPattern());
        }

        // Ditto.
        if (elt instanceof ElementPathBlock) {
            return compilePathBlock(((ElementPathBlock) elt).getPattern());
        }

        if (elt instanceof ElementSubQuery) {
            return compileElementSubquery((ElementSubQuery) elt);
        }

        if (elt instanceof ElementData) {
            return compileElementData((ElementData) elt);
        }

        if (elt instanceof ElementHistory) {
            return compileElementHistory((ElementHistory) elt);
        }

        if (elt == null) {
            return OpNull.create();
        }

        broken("compile(Element)/Not a structural element: " + Utils.className(elt));
        return null;
    }

    //Produce the algebra for a single group.
    //<a href="http://www.w3.org/TR/rdf-sparql-query/#sparqlQuery">Translation to the SPARQL Algebra</a>
    //
    // Step : (URI resolving and triple pattern syntax forms) was done during parsing
    // Step : Collection FILTERS [prepareGroup]
    // Step : (Paths) e.g. simple links become triple patterns. Done later in [compileOneInGroup]
    // Step : (BGPs) Merge PathBlocks - these are SPARQL 1.1's TriplesBlock   [prepareGroup]
    // Step : (BIND/LET) Associate with BGP [??]
    // Step : (Groups and unions) Was done during parsing to get ElementUnion.
    // Step : Graph Patterns [compileOneInGroup]
    // Step : Filters [here]
    // Simplification: Done later 
    // If simplification is done now, it changes OPTIONAL { { ?x :p ?w . FILTER(?w>23) } } because it removes the
    //   (join Z (filter...)) that in turn stops the filter getting moved into the LeftJoin.  
    //   It need a depth of 2 or more {{ }} for this to happen. 
    protected Op compileElementGroup(ElementGroup groupElt) {
        Pair<List<ElementFilter>, List<Element>> pair = prepareGroup(groupElt);
        List<ElementFilter> filters = pair.getLeft();
        List<Element> groupElts = pair.getRight();

        // Compile the consolidated group elements.
        // "current" is the completed part only - there may be thing pushed into the accumulator.
        Op current = OpTable.unit();
        Deque<Op> acc = new ArrayDeque<Op>();

        for (Iterator<Element> iter = groupElts.listIterator(); iter.hasNext();) {
            Element elt = iter.next();
            if (elt != null) {
                current = compileOneInGroup(elt, current, acc);
            }
        }

        // Deal with any remaining ops.
        current = joinOpAcc(current, acc);

        if (filters != null) {
            // Put filters round the group.
            for (ElementFilter filter : filters) {
                current = createFilter(filter, current);
            }
        }
        return current;
    }

    /* Extract filters, merge adjacent BGPs, do BIND.
     * When extracting filters, BGP or PathBlocks may become adjacent
     * so merge them into one. 
     * Return a list of elements with any filters at the end. 
     */
    private Pair<List<ElementFilter>, List<Element>> prepareGroup(ElementGroup groupElt) {
        List<Element> groupElts = new ArrayList<Element>();

        PathBlock currentBGP = null;
        PathBlock currentPathBlock = null;
        List<ElementFilter> filters = null;

        for (Element elt : groupElt.getElements()) {
            if (!fixedFilterPosition && elt instanceof ElementFilter) {
                // For fixed position filters, drop through to general element processing.
                // It's also illegal SPARQL - filters operate over the whole group.
                ElementFilter f = (ElementFilter) elt;
                if (filters == null) {
                    filters = new ArrayList<ElementFilter>();
                }
                filters.add(f);
                // Collect filters but do not place them yet.
                continue;
            }

            // The parser does not generate ElementTriplesBlock (SPARQL 1.1) 
            // but SPARQL 1.0 does and also we cope for programmtically built queries
            if (elt instanceof ElementTriplesBlock) {
                ElementTriplesBlock etb = (ElementTriplesBlock) elt;

                if (currentPathBlock == null) {
                    ElementPathBlock etb2 = new ElementPathBlock();
                    currentPathBlock = etb2.getPattern();
                    groupElts.add(etb2);
                }

                for (Triple t : etb.getPattern()) {
                    currentPathBlock.add(new TriplePath(t));
                }
                continue;
            }

            // To PathLib
            if (elt instanceof ElementPathBlock) {
                ElementPathBlock epb = (ElementPathBlock) elt;
                // Done later.  remove when definitely OK to delay.
//                for ( TriplePath p : epb.getPattern() )
//                {
//                    if ( p.isTriple() )
//                    {}
//                    
//                    // Do the top level conversion.
//                    Path path = p.getPath() ;
//                    if ( path instanceof P_Link )
//                    {}
//                    else if ( path instanceof P_Inverse ) 
//                    {
//                        // and inverse of a link.
//                    }
//                    else if ( path instanceof P_Seq )
//                    {
//                        P_Seq seq = (P_Seq)path ;
//                        
//                    }
//                    else
//                    {}
//                }

                if (currentPathBlock == null) {
                    ElementPathBlock etb2 = new ElementPathBlock();
                    currentPathBlock = etb2.getPattern();
                    groupElts.add(etb2);
                }

                currentPathBlock.addAll(epb.getPattern());
                continue;
            }

            // else
            // Not BGP, path or filters.
            // Clear any BGP-related triple accumulators.
            currentPathBlock = null;
            // Add this element
            groupElts.add(elt);
        }
        return Pair.create(filters, groupElts);
    }

    /**
     * Flush the op accumulator - and clear it
     */
    private void accumulate(Deque<Op> acc, Op op) {
        acc.addLast(op);
    }

    /**
     * Accumulate stored ops, return unit if none.
     */
    private Op popAccumulated(Deque<Op> acc) {
        if (acc.size() == 0) {
            return OpTable.unit();
        }

        Op joined = null;
        // First first to last.
        for (Op op : acc) {
            joined = OpJoin.create(joined, op);
        }
        acc.clear();
        return joined;
    }

    /**
     * Join stored ops to the current state
     */
    private Op joinOpAcc(Op current, Deque<Op> acc) {
        if (acc.size() == 0) {
            return current;
        }
        Op joined = current;
        // First first to last.
        for (Op op : acc) {
            joined = OpJoin.create(joined, op);
        }
        acc.clear();
        return joined;
    }

    private Op compileOneInGroup(Element elt, Op current, Deque<Op> acc) {
        // PUSH
        // Replace triple patterns by OpBGP (i.e. SPARQL translation step 1)
        if (elt instanceof ElementTriplesBlock) {
            // Should not happen.
            ElementTriplesBlock etb = (ElementTriplesBlock) elt;
            Op op = compileBasicPattern(etb.getPattern());
            accumulate(acc, op);
            return current;
        }

        // PUSH
        if (elt instanceof ElementPathBlock) {
            ElementPathBlock epb = (ElementPathBlock) elt;
            Op op = compilePathBlock(epb.getPattern());
            accumulate(acc, op);
            return current;
        }

        // POP
        if (elt instanceof ElementAssign) {
            // This step and the similar BIND step needs to access the preceeding 
            // element if it is a BGP.
            // That might 'current', or in the left side of a join.
            // If not a BGP, insert a empty one.  

            Op op = popAccumulated(acc);
            ElementAssign assign = (ElementAssign) elt;
            Op opAssign = OpAssign.assign(op, assign.getVar(), assign.getExpr());
            accumulate(acc, opAssign);
            return current;
        }

        // POP
        if (elt instanceof ElementBind) {
            Op op = popAccumulated(acc);
            ElementBind bind = (ElementBind) elt;
            Op opExtend = OpExtend.extend(op, bind.getVar(), bind.getExpr());
            accumulate(acc, opExtend);
            return current;
        }

        // Flush.
        current = joinOpAcc(current, acc);

        if (elt instanceof ElementOptional) {
            ElementOptional eltOpt = (ElementOptional) elt;
            return compileElementOptional(eltOpt, current);
        }

//        if ( elt instanceof ElementSubQuery )
//        {
//            ElementSubQuery elQuery = (ElementSubQuery)elt ;
//            Op op = compileElementSubquery(elQuery) ;
//            //TODO Plain join
//            return join(current, op) ;
//        }
        if (elt instanceof ElementMinus) {
            ElementMinus elt2 = (ElementMinus) elt;
            Op op = compileElementMinus(current, elt2);
            return op;
        }

        // All elements that simply "join" into the algebra.
        if (elt instanceof ElementGroup
                || elt instanceof ElementNamedGraph
                || elt instanceof ElementService
                || elt instanceof ElementFetch
                || elt instanceof ElementUnion
                || elt instanceof ElementSubQuery
                || elt instanceof ElementData) {
            Op op = compileElement(elt);
            return join(current, op);
        }

        if (elt instanceof ElementExists) {
            ElementExists elt2 = (ElementExists) elt;
            Op op = compileElementExists(current, elt2);
            return op;
        }

        if (elt instanceof ElementNotExists) {
            ElementNotExists elt2 = (ElementNotExists) elt;
            Op op = compileElementNotExists(current, elt2);
            return op;
        }

        // Filters were collected together by prepareGroup
        // This only handels filters left in place by some magic. 
        if (elt instanceof ElementFilter) {
            return createFilter((ElementFilter) elt, current);
        }

//        // SPARQL 1.1 UNION -- did not make SPARQL 
//        if ( elt instanceof ElementUnion )
//        {
//            ElementUnion elt2 = (ElementUnion)elt ;
//            if ( elt2.getElements().size() == 1 )
//            {
//                Op op = compileElementUnion(current, elt2) ;
//                return op ;
//            }
//        }
        broken("compile/Element not recognized: " + Utils.className(elt));
        return null;
    }

    private Op compileElementUnion(ElementUnion el) {
        Op current = null;

        for (Element subElt : el.getElements()) {
            Op op = compileElement(subElt);
            current = union(current, op);
        }
        return current;
    }

    private Op compileElementNotExists(Op current, ElementNotExists elt2) {
        Op op = compile(elt2.getElement());    // "compile", not "compileElement" -- do simpliifcation  
        Expr expr = new E_Exists(elt2, op);
        expr = new E_LogicalNot(expr);
        return OpFilter.filter(expr, current);
    }

    private Op compileElementExists(Op current, ElementExists elt2) {
        Op op = compile(elt2.getElement());    // "compile", not "compileElement" -- do simpliifcation 
        Expr expr = new E_Exists(elt2, op);
        return OpFilter.filter(expr, current);
    }

    private Op createFilter(ElementFilter filter, Op current) {
        if (filter instanceof ElementFilterWithHistory) {
            return new OpFilterWithHistory(filter.getExpr(), current, ((ElementFilterWithHistory) filter).getVarName());
        }
        return OpFilter.filter(filter.getExpr(), current);
    }

    private Op compileElementMinus(Op current, ElementMinus elt2) {
        Op op = compile(elt2.getMinusElement());
        Op opMinus = OpMinus.create(current, op);
        return opMinus;
    }

    private Op compileElementData(ElementData elt) {
        return OpTable.create(elt.getTable());
    }

    private Op compileElementHistory(ElementHistory elt) {
        return new OpHistory("");
    }

    private Op compileElementUnion(Op current, ElementUnion elt2) {
        // Special SPARQL 1.1 case.
        Op op = compile(elt2.getElements().get(0));
        Op opUnion = OpUnion.create(current, op);
        return opUnion;
    }

    protected Op compileElementOptional(ElementOptional eltOpt, Op current) {
        Element subElt = eltOpt.getOptionalElement();
        Op op = compileElement(subElt);

        ExprList exprs = null;
        if (op instanceof OpFilter) {
            OpFilter f = (OpFilter) op;
            //f = OpFilter.tidy(f) ;  // Collapse filter(filter(..))
            Op sub = f.getSubOp();
            if (sub instanceof OpFilter) {
                broken("compile/Optional/nested filters - unfinished");
            }
            exprs = f.getExprs();
            op = sub;
        }
        current = OpLeftJoin.create(current, op, exprs);
        return current;
    }

    protected Op compileBasicPattern(BasicPattern pattern) {
        return new OpBGP(pattern);
    }

    protected Op compilePathBlock(PathBlock pathBlock) {
        // Empty path block : the parser does not generate this case.
        if (pathBlock.size() == 0) {
            return OpTable.unit();
        }

        // Always turns the most basic paths to triples.
        return PathLib.pathToTriples(pathBlock);
    }

    protected Op compileElementGraph(ElementNamedGraph eltGraph) {
        Node graphNode = eltGraph.getGraphNameNode();
        Op sub = compileElement(eltGraph.getElement());
        return new OpGraph(graphNode, sub);
    }

    protected Op compileElementService(ElementService eltService) {
        Node serviceNode = eltService.getServiceNode();
        Op sub = compileElement(eltService.getElement());
        return new OpService(serviceNode, sub, eltService, eltService.getSilent());
    }

    private Op compileElementFetch(ElementFetch elt) {
        Node serviceNode = elt.getFetchNode();

        // Probe to see if enabled.
        OpExtBuilder builder = OpExtRegistry.builder("fetch");
        if (builder == null) {
            Log.warn(this, "Attempt to use OpFetch - need to enable first with a call to OpFetch.enable()");
            return OpLabel.create("fetch/" + serviceNode, OpTable.unit());
        }
        Item item = Item.createNode(elt.getFetchNode());
        ItemList args = new ItemList();
        args.add(item);
        return builder.make(args);
    }

    protected Op compileElementSubquery(ElementSubQuery eltSubQuery) {
        return compile(eltSubQuery.getQuery());
        //AlgebraGenerator gen = new AlgebraGenerator(context, subQueryDepth+1) ;
        //return gen.compile(eltSubQuery.getQuery()) ;
    }

    protected Op compileElementRefreshableNamedGraph(ElementRefreshableNamedGraph refreshGraphElt) {
        return new OpRefreshableGraph((OpGraph) compileElementGraph(refreshGraphElt), refreshGraphElt.getDuration());
    }

    protected Op compileElementRefreshableService(ElementRefreshableService refreshServiceElt) {
        return new OpRefreshableService((OpService) compileElementService(refreshServiceElt), refreshServiceElt.getDuration());
    }

    protected Op compileElementRefreshableStream(ElementRefreshableStream refreshStreamElt) {
        return new OpRefreshableStream((OpStream) compileElementStream(refreshStreamElt), refreshStreamElt.getDuration());
    }

    protected Op compileElementFilterWithHistory(ElementFilterWithHistory history) {
        //return new OpFilterWithHistory(null, null)
        return OpNull.create();
    }

    protected Op compileElementStream(ElementStream streamElt) {
        Node graphNode = streamElt.getGraphNameNode();
        final BasicPattern pattern = new BasicPattern();
        final ElementGroup subElements = new ElementGroup();
        if (streamElt.getElement() instanceof ElementGroup) {
            for (Element element : ((ElementGroup) streamElt.getElement()).getElements()) {
                if (element instanceof ElementTriplesBlock) {
                    pattern.addAll(((ElementTriplesBlock) element).getPattern());
                } else if (element instanceof ElementPathBlock) {
                    for (TriplePath path : ((ElementPathBlock) element).getPattern()) {
                        pattern.add(path.asTriple());
                    }
                } else {
                    subElements.addElement(element);
                }
            }
        } else if (streamElt.getElement() instanceof ElementTriplesBlock) {
            pattern.addAll(((ElementTriplesBlock) streamElt.getElement()).getPattern());
        } else if (streamElt.getElement() instanceof ElementPathBlock) {
            for (TriplePath path : ((ElementPathBlock) streamElt.getElement()).getPattern()) {
                pattern.add(path.asTriple());
            }
        } else {
            subElements.addElement(streamElt.getElement());
        }
        Op sub = compileElement(subElements);
        return new OpStream(graphNode, sub, pattern, streamElt.getWindowInfo());
    }

    /**
     * Compile query modifiers
     */
    private Op compileModifiers(Query query, Op pattern) {
        /* The modifier order in algebra is:
         * 
         * Limit/Offset
         *   Distinct/reduce
         *     project
         *       OrderBy
         *         Bindings
         *           having
         *             select expressions
         *               group
         */

        // Preparation: sort SELECT clause into assignments and projects.
        VarExprList projectVars = query.getProject();

        VarExprList exprs = new VarExprList();     // Assignments to be done.
        List<Var> vars = new ArrayList<Var>();     // projection variables

        Op op = pattern;

        // ---- GROUP BY
        if (query.hasGroupBy()) {
            // When there is no GroupBy but there are some aggregates, it's a group of no variables.
            op = new OpGroup(op, query.getGroupBy(), query.getAggregators());
        }

        //---- Assignments from SELECT and other places (so available to ORDER and HAVING)
        // Now do assignments from expressions 
        // Must be after "group by" has introduced it's variables.
        // Look for assignments in SELECT expressions.
        if (!projectVars.isEmpty() && !query.isQueryResultStar()) {
            // Don't project for QueryResultStar so initial bindings show
            // through in SELECT *
            if (projectVars.size() == 0 && query.isSelectType()) {
                Log.warn(this, "No project variables");
            }
            // Separate assignments and variable projection.
            for (Var v : query.getProject().getVars()) {
                Expr e = query.getProject().getExpr(v);
                if (e != null) {
                    Expr e2 = ExprLib.replaceAggregateByVariable(e);
                    exprs.add(v, e2);
                }
                // Include in project
                vars.add(v);
            }
        }

        // ---- Assignments from SELECT and other places (so available to ORDER and HAVING)
        if (!exprs.isEmpty()) // Potential rewrites based of assign introducing aliases.
        {
            op = OpExtend.extend(op, exprs);
        }

        // ---- HAVING
        if (query.hasHaving()) {
            for (Expr expr : query.getHavingExprs()) {
                // HAVING expression to refer to the aggregate via the variable.
                Expr expr2 = ExprLib.replaceAggregateByVariable(expr);
                op = OpFilter.filter(expr2, op);
            }
        }
        // ---- VALUES
        if (query.hasValues()) {
            Table table = TableFactory.create(query.getValuesVariables());
            for (Binding binding : query.getValuesData()) {
                table.addBinding(binding);
            }
            OpTable opTable = OpTable.create(table);
            op = OpJoin.create(op, opTable);
        }

        // ---- ToList
        if (context.isTrue(ARQ.generateToList)) // Listify it.
        {
            op = new OpList(op);
        }

        // ---- ORDER BY
        if (query.getOrderBy() != null) {
            List<SortCondition> scList = new ArrayList<SortCondition>();

            // Aggregates in ORDER BY
            for (SortCondition sc : query.getOrderBy()) {
                Expr e = sc.getExpression();
                e = ExprLib.replaceAggregateByVariable(e);
                scList.add(new SortCondition(e, sc.getDirection()));

            }
            op = new OpOrder(op, scList);
        }

        // ---- PROJECT
        // No projection => initial variables are exposed.
        // Needed for CONSTRUCT and initial bindings + SELECT *
        if (vars.size() > 0) {
            op = new OpProject(op, vars);
        }

        // ---- DISTINCT
        if (query.isDistinct()) {
            op = OpDistinct.create(op);
        }

        // ---- REDUCED
        if (query.isReduced()) {
            op = OpReduced.create(op);
        }

        // ---- LIMIT/OFFSET
        if (query.hasLimit() || query.hasOffset()) {
            op = new OpSlice(op, query.getOffset() /*start*/, query.getLimit()/*length*/);
        }

        return op;
    }

    // -------- 
    private static Op join(Op current, Op newOp) {
        if (simplifyTooEarlyInAlgebraGeneration && applySimplification) {
            return OpJoin.createReduce(current, newOp);
        }

        return OpJoin.create(current, newOp);
    }

    protected Op sequence(Op current, Op newOp) {
        return OpSequence.create(current, newOp);
    }

    protected Op union(Op current, Op newOp) {
        return OpUnion.create(current, newOp);
    }

    private void broken(String msg) {
        //System.err.println("AlgebraGenerator: "+msg) ;
        throw new ARQInternalErrorException(msg);
    }
}