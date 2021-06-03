import java.io.*;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

public class GridIndex3 {
    public String getTableName() {
        return tableName;
    }

    private String pk;
    private String tableName;

    private Vector<Vector<String>> gridList;

    private Vector<String> columnNames;

    private Vector<Vector<Comparable>> columnRanges;

    public Vector<Vector<String>> getGridList() {
        return gridList;
    }

    public Vector<String> getColumnNames() {
        return columnNames;
    }

    public Vector<Vector<Comparable>> getColumnRanges() {
        return columnRanges;
    }


    public GridIndex3(String tableName, String[] columnNames, Vector<Vector<Comparable>> columnRanges, String pk) {
        this.tableName = tableName;
        this.gridList = new Vector<>();
        for (int i = 0; i < columnNames.length; i++) {
            this.gridList.addElement(new Vector<>(10));
        }
        this.columnNames = new Vector<>();
        Collections.addAll(this.columnNames, columnNames);
        this.columnRanges = columnRanges;
        this.pk = pk;
    }

    public void deleteFromGrid(Tuple t) {
        for (int i = 0; i < columnNames.size(); i++) {
            Comparable c = t.getEntries().get(columnNames.elementAt(i));
            if (c != null) {
                //find the range
                int indexofRange = indexOfRange(columnRanges.elementAt(i), c);
                deleteFromRange(i, indexofRange, t.getClusteringKey());
            }
        }
    }

    private void deleteFromRange(int i, int indexofRange, Comparable key) {
        Bucket bucket = deserializeBucket(gridList.elementAt(i).elementAt(indexofRange));
        Bucket prevBucket = null;
        while (bucket.bucketBody.size() == DBApp.B && bucket.bucketBody.lastElement().getClusteringKey().compareTo(key) < 0) {
            prevBucket = bucket;
            bucket = deserializeBucket(bucket.overFlow);
        }
        //index of bucket entry to delete
        int j = getBucketEntryIndex(bucket, key);
        bucket.bucketBody.removeElementAt(j);
        if (bucket.overFlow.equals("")) {
            if (bucket.bucketBody.isEmpty()) {
                if (prevBucket == null) {
                    gridList.elementAt(i).setElementAt(null, indexofRange);
                    deleteBucket(bucket.getBucketname());
                    return;
                } else {
                    prevBucket.setOverFlow("");
                    serializeBucket(prevBucket.getBucketname(), prevBucket);
                }
                deleteBucket(bucket.getBucketname());
            }
        } else {
            bucket.bucketBody.addElement(shiftOverFlow(bucket, bucket.overFlow));
        }
        serializeBucket(bucket.getBucketname(), bucket);
    }

    private void deleteBucket(String bName) {
        File f = new File("src/main/resources/data/" + bName + ".class");
        f.delete();
    }

    private BucketEntry shiftOverFlow(Bucket prevBucket, String of) {
        Bucket bucket = deserializeBucket(of);
        if (!bucket.overFlow.equals(""))
            bucket.bucketBody.addElement(shiftOverFlow(bucket, prevBucket.overFlow));
        BucketEntry firstElement = bucket.bucketBody.remove(0);
        if (bucket.bucketBody.isEmpty()) {
            prevBucket.setOverFlow("");
            deleteBucket(bucket.getBucketname());
        } else {
            serializeBucket(bucket.getBucketname(), bucket);
        }
        return firstElement;

    }

    public void insertInGrid(Tuple t, String pageName) {
        for (int i = 0; i < columnNames.size(); i++) {
            Comparable c = t.getEntries().get(columnNames.elementAt(i));
            if (c != null) {
                //find the range
                int indexofRange = indexOfRange(columnRanges.elementAt(i), c);
                insertInRange(i, indexofRange, t, pageName, c);
            }
        }
    }

    private void insertInRange(int i, int indexofRange, Tuple t, String pageName, Comparable c) {
        BucketEntry newEntry = new BucketEntry(t.getClusteringKey(), pageName, null);
        //check if bucket exist:
        if (gridList.elementAt(i).elementAt(indexofRange) == null) {
            String temp = getnewBucketName(tableName);
            Bucket b = new Bucket(temp);
            b.bucketBody.addElement(newEntry);
            gridList.elementAt(i).setElementAt(temp, indexofRange);
            serializeBucket(gridList.elementAt(i).elementAt(indexofRange), b);
        } else {
            Bucket currentBucket = deserializeBucket(gridList.elementAt(i).elementAt(indexofRange));
            if (currentBucket.bucketBody.size() == DBApp.B) {
                //Bucket is Full
                BucketEntry tobeinserted = currentBucket.bucketBody.lastElement();
                if (tobeinserted.compareTo(newEntry) > 0) {
                    currentBucket.bucketBody.removeElementAt(DBApp.B - 1);
                    currentBucket.insertBucketEntry(newEntry);
                } else {
                    tobeinserted = newEntry;
                }
                String ofname = insertInOverFlow(currentBucket.getOverFlow(), tobeinserted);
                currentBucket.setOverFlow(ofname);
            } else {
                //Bucket is not Full
                currentBucket.insertBucketEntry(newEntry);
            }
            serializeBucket(gridList.elementAt(i).elementAt(indexofRange), currentBucket);

        }

    }

    private String insertInOverFlow(String overflow, BucketEntry newEntry) {
        if (overflow.equals("")) {
            String tempname = getnewBucketName(tableName);
            Bucket b = new Bucket(tempname);
            b.insertBucketEntry(newEntry);
            serializeBucket(tempname, b);
            return tempname;
        } else {
            Bucket ovfl = deserializeBucket(overflow);
            if (ovfl.bucketBody.size() < DBApp.B) {
                ovfl.insertBucketEntry(newEntry);
                serializeBucket(overflow, ovfl);
            } else {
                BucketEntry tobeinserted = ovfl.bucketBody.lastElement();
                if (tobeinserted.compareTo(newEntry) > 0) {
                    ovfl.bucketBody.removeElementAt(DBApp.B - 1);
                    ovfl.insertBucketEntry(newEntry);
                } else {
                    tobeinserted = newEntry;
                }
                String ofname = insertInOverFlow(ovfl.getOverFlow(), tobeinserted);
                ovfl.setOverFlow(ofname);
                serializeBucket(overflow, ovfl);
            }
        }
        return overflow;
    }

    public void updatePageName(Tuple tuple, String pageName) {
        for (int i = 0; i < columnNames.size(); i++) {
            Comparable c = tuple.getEntries().get(columnNames.elementAt(i));
            if (c != null) {
                //find the range
                int indexofRange = indexOfRange(columnRanges.elementAt(i), c);
                Bucket bucket = deserializeBucket(gridList.elementAt(i).elementAt(indexofRange));
                while (bucket.bucketBody.size() == DBApp.B && bucket.bucketBody.lastElement().getClusteringKey().compareTo(tuple.getClusteringKey()) < 0) {
                    bucket = deserializeBucket(bucket.overFlow);
                }
                bucket.bucketBody.elementAt(getBucketEntryIndex(bucket, tuple.getClusteringKey())).setPageName(pageName);
                serializeBucket(bucket.getBucketname(), bucket);
            }
        }

    }

    private int getBucketEntryIndex(Bucket bucket, Comparable clusteringKey) {
        int lo = 0;
        int hi = bucket.bucketBody.size() - 1;
        int i;
        while (lo <= hi) {
            i = (lo + hi) / 2;
            if (clusteringKey.compareTo(bucket.getBucketBody().elementAt(i).getClusteringKey()) == 0) {
                return i;
            } else if (clusteringKey.compareTo(bucket.getBucketBody().elementAt(i).getClusteringKey()) < 0) {
                hi = i - 1;
            } else {
                lo = i + 1;
            }
        }
        return -1;
    }

    public int indexOfRange(Vector<Comparable> ranges, Comparable value) {
        int lo = 0;
        int hi = ranges.size() - 1;
        int i = -1;
        while (lo <= hi) {
            i = (lo + hi) / 2;
            if (value.compareTo(ranges.elementAt(i)) < 0) {
                hi = i - 1;
            } else {
                if (i != ranges.size() - 1) {
                    if (value.compareTo(ranges.elementAt(i + 1)) < 0) {
                        return i;
                    } else {
                        lo = i + 1;
                    }
                } else {
                    return i;
                }
            }
        }
        return i;
    }

    private void serializeBucket(String name, Bucket b) {
        try {
            FileOutputStream fileout = new FileOutputStream("src/main/resources/data/" + name + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileout);
            out.writeObject(b);
            out.close();
            fileout.close();
        } catch (IOException i) {
            i.getMessage();
        }
    }

    private Bucket deserializeBucket(String name) {
        Bucket b = null;
        try {
            FileInputStream filein = new FileInputStream("src/main/resources/data/" + name + ".class");
            ObjectInputStream in = new ObjectInputStream(filein);
            b = (Bucket) in.readObject();
            in.close();
            filein.close();
        } catch (IOException | ClassNotFoundException i) {
            i.getMessage();
        }
        return b;
    }

    private String getnewBucketName(String tableName) {
        LocalDateTime now = LocalDateTime.now();
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return tableName + "Bucket" + now.getDayOfYear() + now.getHour() + now.getMinute() + now.getSecond() + now.getNano();
    }

    public String getPageNameFromIndex(Comparable clusteringKey) {
        int pkindex = columnNames.indexOf(pk);
        int indexofRange = indexOfRange(getColumnRanges().elementAt(pkindex), clusteringKey);
        if (gridList.elementAt(pkindex).elementAt(indexofRange) != null) {
            Bucket bucket = deserializeBucket(gridList.elementAt(pkindex).elementAt(indexofRange));
            Bucket prevBucket = null;
            while (bucket.bucketBody.size() == DBApp.B && bucket.bucketBody.lastElement().getClusteringKey().compareTo(clusteringKey) < 0) {
                prevBucket = bucket;
                bucket = deserializeBucket(bucket.overFlow);
            }
            int i = indexToInsertAt(clusteringKey, bucket.bucketBody);
            i--;
            if (i != -1) {
                return bucket.getBucketBody().elementAt(i).getPageName();
            } else {
                if (prevBucket != null)
                    return prevBucket.bucketBody.lastElement().getPageName();
                else {
                    String p = getLargestOfSmaller(pkindex,indexofRange);
                    if(p!= null){
                        return p;
                    }else{
                        return bucket.bucketBody.firstElement().getPageName();
                    }
                }
            }

        } else {
            String p = getLargestOfSmaller(pkindex,indexofRange);
            if(p!= null){
                return p;
            }else{
                return getSmallestOfLarger(pkindex,indexofRange);
            }
        }
    }

    private String getLargestOfSmaller(int i, int indexOfRange) {
        if (indexOfRange < 0) return null;
        if (null == gridList.elementAt(i).elementAt(indexOfRange)) {
            getLargestOfSmaller(i, indexOfRange - 1);
        } else {
            Bucket bucket = deserializeBucket(gridList.elementAt(i).elementAt(indexOfRange));
            while (!bucket.overFlow.equals("")) {
                bucket = deserializeBucket(bucket.overFlow);
            }
            return bucket.bucketBody.lastElement().getPageName();
        }
        return null;
    }

    private String getSmallestOfLarger(int i, int indexOfRange) {
        if (indexOfRange > 9) return null;
        if (null == gridList.elementAt(i).elementAt(indexOfRange)) {
            getLargestOfSmaller(i, indexOfRange + 1);
        } else {
            Bucket bucket = deserializeBucket(gridList.elementAt(i).elementAt(indexOfRange));
            return bucket.bucketBody.firstElement().getPageName();
        }
        return null;

    }

    private int indexToInsertAt(Comparable key, Vector<BucketEntry> keysInPage) {
        int lo = 0;
        int hi = keysInPage.size() - 1;
        while (lo <= hi) {
            int i = (lo + hi) / 2;
            if (key.compareTo(keysInPage.elementAt(i).getClusteringKey()) < 0) {
                hi = i - 1;
            } else {
                lo = i + 1;
            }
        }
        return lo;
    }

    public Vector<String> getNeededPageNames(String tableName, Hashtable<String, Object> columnNameValue, String Operator) {
        Vector<String> columnone = new Vector<String>();
        HashSet<String> hs = new HashSet<>();
        for (String currentColumnName : columnNameValue.keySet()) {
            int i = columnNames.indexOf(currentColumnName);
            int rangeindex = indexOfRange(columnRanges.elementAt(i), (Comparable) columnNameValue.get(currentColumnName));
            // getPageNamesForColumnValue(i,rangeindex,(Comparable) columnNameValue.get(currentColumnName),hs);
        }
        return columnone;
    }

    public void changeBucket(Tuple old, Tuple neo, String pagename) {

        for (int i = 0; i < columnNames.size(); i++) {
            Comparable cold = old.getEntries().get(columnNames.elementAt(i));
            Comparable cneo = old.getEntries().get(columnNames.elementAt(i));
            if (cold != null && cneo != null) {
                int indexofRangeold = indexOfRange(columnRanges.elementAt(i), cold);
                int indexofRangeneo = indexOfRange(columnRanges.elementAt(i), cneo);
                if (indexofRangeold != indexofRangeneo) {
                    deleteFromRange(i, indexofRangeold, old);
                    insertInRange(i, indexofRangeneo, neo, pagename, cneo);
                }
            } else if (cold == null && cneo != null) {
                int indexofRangeneo = indexOfRange(columnRanges.elementAt(i), cneo);
                insertInRange(i, indexofRangeneo, neo, pagename, cneo);
            }
        }
    }

    // public getPageNamesForColumnValue(int i, int range, Comparable columnnamval,HashSet<String> hs)
    {

    }
}

