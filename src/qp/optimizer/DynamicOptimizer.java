package qp.optimizer;

import qp.operators.*;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.SQLQuery;
import qp.utils.Schema;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.*;

public class DynamicOptimizer {
    SQLQuery sqlquery;
    int numOfTable;
    Vector<HashMap<Set<String>, Operator>> subsetSpace;

    public DynamicOptimizer(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;
        this.numOfTable = sqlquery.getFromList().size();
        subsetSpace = new Vector<HashMap<Set<String>, Operator>>(this.numOfTable);
        HashMap<Set<String>, Operator> map;
        for (int i = 0; i < this.numOfTable; i++) {
            map = new HashMap<Set<String>, Operator>();
            subsetSpace.add(map);
        }
    }

    //attatch all operators on the basic layer (|s| == 1)
    private void initializeBasicLayer() {
        Operator curOp;
        HashSet<String> curRelation;
        String tabName;
        for (int i = 0; i < numOfTable; i++) {
            curRelation = new HashSet<String>();
            curRelation.add((String) sqlquery.getFromList().elementAt(i));
            //get the operator for this single relation
            tabName = (String) sqlquery.getFromList().elementAt(i);
            curOp = createSelectOp(createScanOp(tabName), tabName);
            //fill in the basic level
            subsetSpace.elementAt(0).put(curRelation, curOp);
            System.out.println(curRelation);
            Debug.PPrint(curOp);
            System.out.println("");
        }
    }

    //for the basic layer, attatch a scan operator on each of them
    private Operator createScanOp(String tabName) {
        Scan scanOp = null;
        // same as in the random optimizer
        String filename = tabName + ".md";
        try {
            ObjectInputStream _if = new ObjectInputStream(new FileInputStream(filename));
            Schema schm = (Schema) _if.readObject();
            scanOp = new Scan(tabName, OpType.SCAN);
            scanOp.setSchema(schm);
            _if.close();
        } catch (Exception e) {
            System.err.println("createBasicLayerOp: Error reading Schema of the table " + filename);
            System.exit(1);
        }
        return scanOp;
    }

    //attatch select operator on scan operator if there exists one
    private Operator createSelectOp(Operator scanOp, String tabName) {
        Select selectOp = null;
        for (int j = 0; j < sqlquery.getSelectionList().size(); j++) {
            //System.out.println("inside select op");
            Condition cn = (Condition) sqlquery.getSelectionList().elementAt(j);
            if (tabName.equals(cn.getLhs().getTabName())) {
                selectOp = new Select(scanOp, cn, OpType.SELECT);
                selectOp.setSchema(scanOp.getSchema());
                return selectOp;
            }
        }
        return scanOp;
    }

    //attach project operator at last
    private Operator createProjectOp(Operator op){
        Operator projectOp = null;
        Vector<Attribute> projectList = (Vector<Attribute>) sqlquery.getProjectList();
        if (projectList != null && !projectList.isEmpty()) {
            projectOp = new Project(op, projectList, OpType.PROJECT);
            Schema newSchema = op.getSchema().subSchema(projectList);
            projectOp.setSchema(newSchema);
            return projectOp;
        }
        return op;
    }

    /**
     *
     * @param leftTablesRoot which is the root operator of the current tree
     * @param oneTable the single table
     * @return the join condition between these two group
     */
    private Condition getJoinCondition(Operator leftTablesRoot, Set<String> oneTable) {
        Condition condition;
        Attribute lhsAttr;
        Attribute rhsAttr;
        Condition result;

        for (int i = 0; i < sqlquery.getJoinList().size(); i++) {
            condition = (Condition) sqlquery.getJoinList().elementAt(i);
            lhsAttr = condition.getLhs();
            rhsAttr = (Attribute) condition.getRhs();

            if ((leftTablesRoot.getSchema().contains(lhsAttr)
                && (subsetSpace.elementAt(0).get(oneTable).getSchema().contains(rhsAttr)))) {
                return (Condition) condition.clone();
            } else if (leftTablesRoot.getSchema().contains(rhsAttr)
                       && subsetSpace.elementAt(0).get(oneTable).getSchema().contains(lhsAttr)) {
                result = (Condition) condition.clone();
                result.setRhs((Attribute) lhsAttr.clone());
                result.setLhs((Attribute) rhsAttr.clone());
                return result;
            }
        }
        return null;
    }

    /** as stated in the lecture notes,
     * accessPlan will give the best plan for a subset
    **/
    private Operator accessPlan(String[] subset) {
        int MINCOST = Integer.MAX_VALUE;
        Operator startOp = null;
        HashSet<String> oneTable;
        HashSet<String> leftTables;
        String tabName;
        Operator op;
        Condition con;
        Join joinOp;
        Schema schema;
        int planCost;
        int curCost;
        PlanCost pc;
        int type;

        //calculate the cost for each of the combination
        for (int i = 0; i < subset.length; i++) {

            tabName = subset[i];
            oneTable = new HashSet<String>();
            oneTable.add(tabName);
            leftTables = new HashSet<String>();

            for (int j = 0; j < subset.length; j++) {
                if (j != i)
                    leftTables.add(subset[j]);
            }

            op = subsetSpace.elementAt(subset.length - 2).get(leftTables);

            if (op == null) {
                continue;
            }

            con = getJoinCondition(op, oneTable);
            if (con != null) {
                joinOp = new Join(op, subsetSpace.elementAt(0).get(oneTable), con, OpType.JOIN);
                schema = op.getSchema().joinWith(subsetSpace.elementAt(0).get(oneTable).getSchema());
                joinOp.setSchema(schema);

                curCost = 0;
                planCost = Integer.MAX_VALUE;
                type = 1;

                //same as in the random optimizer
//                joinOp.setJoinType(JoinType.BLOCKNESTED);
//                pc = new PlanCost();
//                curCost = pc.getCost(joinOp);
//                if (curCost < planCost) {
//                    planCost = curCost;
//                    type = JoinType.BLOCKNESTED;
//                }

                joinOp.setJoinType(JoinType.NESTEDJOIN);
                pc = new PlanCost();
                curCost = pc.getCost(joinOp);
                if (curCost < planCost) {
                    planCost = curCost;
                    type = JoinType.NESTEDJOIN;
                }

//                joinOp.setJoinType(JoinType.INDEXNESTED);
//                pc = new PlanCost();
//                curCost = pc.getCost(joinOp);
//                if (curCost < planCost) {
//                    planCost = curCost;
//                    type = JoinType.INDEXNESTED;
//                }
//
//                joinOp.setJoinType(JoinType.HASHJOIN);
//                pc = new PlanCost();
//                curCost = pc.getCost(joinOp);
//                if (curCost < planCost) {
//                    planCost = curCost;
//                    type = JoinType.HASHJOIN;
//                }

//				joinOp.setJoinType(JoinType.SORTMERGE);
//				pc = new PlanCost();
//				curCost = pc.getCost(joinOp);
//				if (curCost < planCost) {
//					planCost = curCost;
//					type = JoinType.SORTMERGE;
//				}

                joinOp.setJoinType(type);

                if (planCost < MINCOST) {
                    MINCOST = planCost;
                    startOp = joinOp;
                }
            }
        }

        System.out.print("cost: " + MINCOST + " ");
        return startOp;
    }

    /**
     * this method firstly compute the total number of combinations
     * then convert each number to its binary representation
     * for example if the size is 4
     * then possible combinations are
     * 0000, 0001, 0010, 0011, 0100, 0101, 0110, 1000, 1001, 1010, 1011, 1100, 1101, 1110,1111
     * construct according to the corresponding present of the From
     *
     * @param size which is the size of this subset
     * @return return a hashmap containing all the possible combinations
     */
    private HashMap<Integer, HashSet<String>> getAllSubsets(int size) {
        HashMap<Integer, HashSet<String>> allSubsets = new HashMap<Integer, HashSet<String>>();
        //num of total combinations
        int numOfSubset = (int) Math.pow(2, numOfTable);
        int key = 0;
        String binary;
        int ones;

        for (int i = 0; i < numOfSubset; i++) {
            binary = Integer.toBinaryString(i);
            ones = 0;
            for(int j = 0; j < binary.length(); j++) {
                if(binary.charAt(j) == '1') {
                    ones++;
                }
            }

            if (size == ones) {//one possible combination
                HashSet<String> subSet = new HashSet<String>();
                for (int k = 0; k < numOfTable; k++) {//find the corresponding table name and add into the hashset
                    if(k < binary.length()) {
                        if (binary.charAt(binary.length() - 1 - k) == '1') {
                            subSet.add((String) sqlquery.getFromList().get(k));
                        }
                    }
                }
                allSubsets.put(key, subSet);
                key++;
            }
        }
        return allSubsets;
    }

    //same as in the random optimizer
    public static Operator makeExecPlan(Operator node) {

        if (node.getOpType() == OpType.JOIN) {
            Operator left = makeExecPlan(((Join) node).getLeft());
            Operator right = makeExecPlan(((Join) node).getRight());
            int joinType = ((Join) node).getJoinType();
            int numbuff = BufferManager.getBuffersPerJoin();
            switch (joinType) {
                case JoinType.NESTEDJOIN:

                    NestedJoin nj = new NestedJoin((Join) node);
                    nj.setLeft(left);
                    nj.setRight(right);
                    nj.setNumBuff(numbuff);
                    return nj;

                case JoinType.BLOCKNESTED:

                    BlockNestedJoin bj = new BlockNestedJoin((Join) node);
                    bj.setLeft(left);
                    bj.setRight(right);
                    bj.setNumBuff(numbuff);
                    return bj;
//
//                case JoinType.INDEXNESTED:
//
//                    IndexNested in = new IndexNested((Join) node);
//                    in.setLeft(left);
//                    in.setRight(right);
//                    in.setNumBuff(numbuff);
//                    return in;
//
//                case JoinType.SORTMERGE:
//
//                    SortMergeJoin sm = new SortMergeJoin((Join) node);
//                    sm.setLeft(left);
//                    sm.setRight(right);
//                    sm.setNumBuff(numbuff);
//                    return sm;
//
//                case JoinType.HASHJOIN:
//
//                    HashJoin hj = new HashJoin((Join) node);
//                    hj.setLeft(left);
//                    hj.setRight(right);
//                    hj.setNumBuff(numbuff);
//                    return hj; case JoinType.BLOCKNESTED:
//
//                    BlockNested bj = new BlockNested((Join) node);
//                    bj.setLeft(left);
//                    bj.setRight(right);
//                    bj.setNumBuff(numbuff);
//                    return bj;
//
//                case JoinType.INDEXNESTED:
//
//                    IndexNested in = new IndexNested((Join) node);
//                    in.setLeft(left);
//                    in.setRight(right);
//                    in.setNumBuff(numbuff);
//                    return in;
//
//                case JoinType.SORTMERGE:
//
//                    SortMergeJoin sm = new SortMergeJoin((Join) node);
//                    sm.setLeft(left);
//                    sm.setRight(right);
//                    sm.setNumBuff(numbuff);
//                    return sm;
//
//                case JoinType.HASHJOIN:
//
//                    HashJoin hj = new HashJoin((Join) node);
//                    hj.setLeft(left);
//                    hj.setRight(right);
//                    hj.setNumBuff(numbuff);
//                    return hj;
                default:
                    return node;
            }
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = makeExecPlan(((Select) node).getBase());
            ((Select) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = makeExecPlan(((Project) node).getBase());
            ((Project) node).setBase(base);
            return node;
        } else {
            return node;
        }
    }

    //the method that is called by main,
    //and it returns the best plan root
    public Operator getOptPlan() {
        // fill in the size 1 relations
        System.out.println("----------------------First Layer-------------------");
        initializeBasicLayer();

        // for |s| = 2 to N
        // make use of the previous level to get the best plan for this level
        for (int i = 1; i < numOfTable; i++) {
            System.out.println("----------------------Next Layer--------------------");
            Map<Integer, HashSet<String>> allSubset = getAllSubsets(i + 1);
            //for current i, get the best plan for the corresponding subset s
            for (int j = 0; j < allSubset.size(); j++) {
                HashSet<String> curSubset = allSubset.get(j);
                String[] curSubsetArray = curSubset.toArray(new String[0]);
                Operator curRoot = accessPlan(curSubsetArray);
                if (curRoot != null) {
                    Debug.PPrint(curRoot);
                    System.out.println("");
                } else {
                    System.out.println("null");
                }


                if(curRoot==null){
                    continue;
                }
                HashSet<String> tableNames = new HashSet<String>();
                for (int k = 0; k <= i; k++) {
                    tableNames.add(curSubsetArray[k]);
                }
                subsetSpace.elementAt(i).put(tableNames, curRoot);
            }
        }

        HashSet<String> tables = new HashSet<String>();
        for (int i = 0; i < sqlquery.getFromList().size(); i++) {
            tables.add((String) sqlquery.getFromList().elementAt(i));
        }
        //find the starting operator of the final plan
        Operator startingPoint = subsetSpace.elementAt(numOfTable - 1).get(tables);

        //attach project operators, and return the starting operator
        return createProjectOp(startingPoint);
    }
}