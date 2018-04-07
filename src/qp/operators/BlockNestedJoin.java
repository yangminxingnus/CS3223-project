package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;

public class BlockNestedJoin extends Join {
    int batchsize;  //Number of tuples per out batch

    /** The following fields are useful during execution of
     ** the NestedJoin operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String rfname;    // The file name where the right table is materialize

    static int filenum=0;   // To get unique filenum for this operation

    Batch outbatch;   // Output buffer
    Batch[] leftbatch;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lcurs;    // Cursor for left side buffer
    int rcurs;    // Cursor for right side buffer
    int ocurs;
    int icurs;
    boolean eosl;  // Whether end of stream (left table) is reached
    boolean eosr;  // End of stream (right table)

    int outerBlockSize, leftbatchSize, totalTuples;

    public BlockNestedJoin(Join jn){
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

        int leftTupleSize = left.getSchema().getTupleSize();
        leftbatch = new Batch[numBuff-2];

        Attribute leftattr = con.getLhs();
        Attribute rightattr =(Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;
        /** initialize the cursors of input buffers **/

        lcurs = 0; rcurs =0; ocurs = 0; icurs = 0;
        eosl=false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr=true;

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
            rfname = "BNLtemp-" + String.valueOf(filenum);
            try{
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while( (rightpage = right.next()) != null){
                    out.writeObject(rightpage);
                }
                out.close();
            }catch(IOException io){
                System.out.println("BlockNestedJoin:writing the temporary file error");
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
        //System.out.print("BlockedNestedJoin:--------------------------in next----------------");
        //Debug.PPrint(con);
        //System.out.println();
        int i,j;
        if(eosl){
            close();
            return null;
        }
        outbatch = new Batch(batchsize);

        while(!outbatch.isFull()){

            if(ocurs==0 && icurs == 0 && eosr==true){
                /** a block of left pages is to be fetched**/
                int tmp;
                for(tmp =0;tmp<numBuff -2; tmp++) {
                    leftbatch[tmp] = (Batch) left.next();
                    if(leftbatch[tmp] == null) {
                        if(tmp == 0) {
                            eosl = true;
                            return outbatch;
                        }
                        break;
                    }
                }
                outerBlockSize = tmp;
                /** Whenver a new block of left pages arrives, we retrieve
                 ** the right table page by page.
                 **/
                try{

                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr=false;
                }catch(IOException io){
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }

            }

            while(eosr==false){

                try{
                    if(rcurs==0 && icurs==0 && ocurs==0){
                        rightbatch = (Batch) in.readObject();
                    }
                    for(int k = ocurs; k < outerBlockSize; k++) {
                        for (i = icurs; i < leftbatch[k].size(); i++) {
                            for (j = rcurs; j < rightbatch.size(); j++) {
                                Tuple lefttuple = leftbatch[k].elementAt(i);
                                Tuple righttuple = rightbatch.elementAt(j);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);

                                    //Debug.PPrint(outtuple);
                                    //System.out.println();
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        if (i == leftbatch[k].size() - 1 && j == rightbatch.size() - 1 && k == outerBlockSize - 1) {//case 1
                                            icurs = 0;
                                            rcurs = 0;
                                            ocurs = 0;
                                        } else if(i == leftbatch[k].size() - 1 && j == rightbatch.size() - 1 && k != outerBlockSize - 1) {
                                            icurs = 0;
                                            rcurs = 0;
                                            ocurs = k + 1;
                                        }
                                        else if (i != leftbatch[k].size() - 1 && j == rightbatch.size() - 1) {//case 2
                                            icurs = i + 1;
                                            rcurs = 0;
                                            ocurs = k;
                                        } else {//case 3
                                            icurs = i;
                                            rcurs = j + 1;
                                            ocurs = k;
                                        }
                                        return outbatch;
                                    }
                                }
                            }
                            rcurs = 0;
                        }
                        icurs = 0;
                    }
                    ocurs = 0;
                }catch(EOFException e){
                    try{
                        in.close();
                    }catch (IOException io){
                        System.out.println("BlockNestedJoin:Error in temporary file reading");
                    }
                    eosr=true;
                }catch(ClassNotFoundException c){
                    System.out.println("BlockNestedJoin:Some error in deserialization ");
                    System.exit(1);
                }catch(IOException io){
                    System.out.println("BlockNestedJoin:temporary file reading error");
                    System.exit(1);
                }
            }
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
