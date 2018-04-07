package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;

public class HashJoin extends Join{

    int batchsize;  //Number of tuples per out batch

    /** The following fields are useful during execution of
     ** the NestedJoin operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String rfname;    // The file name where the right table is materialize

    static int filenum=0;   // To get unique filenum for this operation

    Batch outbatch;   // Output buffer
    Batch leftbatch;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lcurs;    // Cursor for left side buffer
    int rcurs;    // Cursor for right side buffer
    int kcurs;
    boolean eosl;  // Whether end of stream (left table) is reached
    boolean eosr;  // End of stream (right table)
    boolean isHashed;
    boolean build;

    Hashtable<Integer, ArrayList<Tuple>> leftHash;
    Hashtable<Integer, ArrayList<Tuple>> rightHash;
    Hashtable<Integer, ArrayList<Tuple>> probeHash;

    ArrayList<Integer> partitionKeys;

    int hash1 = 12289;
    int hash2 = 20173;

    public HashJoin(Join jn){
        super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    /** During open finds the index of the join attributes
     **  Materializes the right hand side into a file
     **  Opens the connections
     **/



    public boolean open(){

        /** select number of tuples per batch **/
        int tuplesize=schema.getTupleSize();
        batchsize=Batch.getPageSize()/tuplesize;

        Attribute leftattr = con.getLhs();
        Attribute rightattr =(Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;

        /** initialize hash tables **/
        leftHash = new Hashtable<>();
        rightHash = new Hashtable<>();
        probeHash = new Hashtable<>();

        partitionKeys = new ArrayList<>();

        /** initialize the cursors of input buffers **/

        lcurs = 0; rcurs =0; kcurs = 0;
        eosl=false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr=false;
        isHashed = false;
        isHashed = false;
        build = false;

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/

        if(!right.open()){
            return false;
        }else{
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/

            //if(right.getOpType() != OpType.SCAN){
            filenum++;
            rfname = "HJtemp-" + String.valueOf(filenum);
            try{
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while( (rightpage = right.next()) != null){
                    out.writeObject(rightpage);
                }
                out.close();
            }catch(IOException io){
                System.out.println("HashJoin:writing the temporary file error");
                return false;
            }
            //}
            if(!right.close())
                return false;
        }
        if(left.open())
            return true;
        else
            return false;
    }



    /** from input buffers selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/


    public Batch next(){
        //System.out.print("HashJoin:--------------------------in next----------------");
        //Debug.PPrint(con);
        //System.out.println();
        int i,j;
        if(isHashed){
            close();
            return null;
        }

        outbatch = new Batch(batchsize);
        /** partitioning phase: partition the left table into a hashtable **/
        if(!eosl){
            /** new left page is to be fetched**/
            leftbatch =(Batch) left.next();
            while(leftbatch!=null) {
            /** Whenever a new left page came , we hash each tuple
             **/
            for(i = 0; i < leftbatch.size(); i++) {
                Tuple lefttuple = leftbatch.elementAt(i);
                int hashValue = lefttuple.dataAt(leftindex).hashCode() % hash1;
                if (leftHash.containsKey(hashValue)) {
                    ArrayList<Tuple> tmp = leftHash.get(hashValue);
                    tmp.add(lefttuple);
                    leftHash.put(hashValue, tmp);
                } else {
                    ArrayList<Tuple> tmp = new ArrayList<>();
                    tmp.add(lefttuple);
                    leftHash.put(hashValue, tmp);
                    partitionKeys.add(hashValue);
                }
            }
            leftbatch = (Batch) left.next();
            }
            eosl = true;
        }
        /** partitioning phase: partition the right table into a hashtable **/
        if(!eosr){
            try{
                in = new ObjectInputStream(new FileInputStream(rfname));
                rightbatch = (Batch) in.readObject();
                /** new right page is to be fetched**/
                while(rightbatch!=null) {
                    /** Whenever a new right page came , we hash each tuple
                     **/
                    for (i = 0; i < rightbatch.size(); i++) {
                        Tuple righttuple = rightbatch.elementAt(i);
                        int hashValue = righttuple.dataAt(rightindex).hashCode() % hash1;
                        if (!rightHash.containsKey(hashValue)) {
                            ArrayList<Tuple> tmp = new ArrayList<>();
                            tmp.add(righttuple);
                            rightHash.put(hashValue, tmp);
                        } else {
                            ArrayList<Tuple> tmp = rightHash.get(hashValue);
                            tmp.add(righttuple);
                            rightHash.put(hashValue, tmp);
                        }
                    }
                    rightbatch = (Batch) in.readObject();
                }
                eosr = true;
            } catch (EOFException e) {
                try {
                    in.close();
                } catch (IOException io) {
                    System.out.println("HashJoin:Error in temporary file reading");
                }
                eosr = true;
            } catch (ClassNotFoundException c) {
                System.out.println("HashJoin:Some error in deserialization ");
                System.exit(1);
            }catch(IOException io){
                System.out.println("HashJoin:temporary file reading error");
                System.out.println(io.getMessage());
                System.exit(1);
            }
        }


        /** joining/probing phase, join 2 hash tables **/
        while(!outbatch.isFull() && !isHashed) {
            for (i = kcurs; i < partitionKeys.size(); i++) {
                int thisKey = partitionKeys.get(i);
                if (!rightHash.containsKey(thisKey)) {
                    kcurs ++;
                    continue;
                }
                ArrayList<Tuple> curLeft = leftHash.get(thisKey);

                if (!build) {
                    probeHash.clear();
                    for (j = 0; j < curLeft.size(); j++) {
                        Tuple cur = curLeft.get(j);
                        int key = cur.dataAt(leftindex).hashCode() % hash2;
                        if (probeHash.containsKey(key)) {
                            ArrayList<Tuple> tmp = probeHash.get(key);
                            tmp.add(cur);
                            probeHash.put(key, tmp);
                        } else {
                            ArrayList<Tuple> tmp = new ArrayList<>();
                            tmp.add(cur);
                            probeHash.put(key, tmp);
                        }
                    }
                    build = true;
                }
                ArrayList<Tuple> curRight = rightHash.get(thisKey);
                for (j = rcurs; j < curRight.size(); j++) {
                    Tuple righttuple = curRight.get(j);
                    int key = righttuple.dataAt(rightindex).hashCode() % hash2;
                    if (probeHash.containsKey(key)) {
                        ArrayList<Tuple> thisLeft = probeHash.get(key);
                        if (!outbatch.isFull()) {
                            for (int k = lcurs; k < thisLeft.size(); k++) {
                                Tuple lefttuple = thisLeft.get(k);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);

                                    //Debug.PPrint(outtuple);
                                    //System.out.println();
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        if (k == thisLeft.size() - 1 && j == curRight.size() - 1) {//case 1
                                            lcurs = 0;
                                            rcurs = 0;
                                            kcurs = i + 1;
                                            build = false;
                                        } else if (k == thisLeft.size() - 1 && j != curRight.size() - 1) {//case 3
                                            lcurs = 0;
                                            rcurs = j + 1;
                                            kcurs = i;
                                        } else {
                                            lcurs = k + 1;
                                            rcurs = j;
                                            kcurs = i;
                                        }
                                        return outbatch;
                                    }
                                }
                            }
                            lcurs = 0;
                        }
                    }
                }
                rcurs = 0;
                kcurs = i + 1;
                build = false;
            }
            if (kcurs >= partitionKeys.size()) isHashed = true;
        }
        return outbatch;
    }



    /** Close the operator */
    public boolean close(){

        File f = new File(rfname);
        f.delete();
        return true;

    }

}
