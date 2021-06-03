import java.io.*;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

public class GridIndex {
    public String getTableName() {
        return tableName;
    }

    private String pk;
    private String tableName;

    private Vector<String> gridList;

    private Vector<String> columnNames;

    private Vector<Vector<Comparable>> columnRanges;

    public Vector<String> getGridList() {
        return gridList;
    }

    public Vector<String> getColumnNames() {
        return columnNames;
    }

    public Vector<Vector<Comparable>> getColumnRanges() {
        return columnRanges;
    }


    public GridIndex(String tableName, String[] columnNames, Vector<Vector<Comparable>> columnRanges, String pk) {
        this.tableName = tableName;
        this.gridList = new Vector<>((int) Math.pow(11, columnNames.length));
        this.columnNames = new Vector<>();
        Collections.addAll(this.columnNames, columnNames);
        this.columnRanges = columnRanges;
        this.pk = pk;
    }

    public int getIndexInGrid(Hashtable<String, Comparable> h) {
        int p = columnNames.size() - 1;
        int index = gridList.size();
        for (int i = 0; i < columnNames.size(); i++) {
            if (h.get(columnNames.elementAt(i)) != null) {
                int rangeindex = indexOfRange(columnRanges.elementAt(i), h.get(columnNames.elementAt(i)));
                index += (int) Math.pow(11, p) * rangeindex;
            } else {
                index += (int) Math.pow(11, p) * 10;
            }
            p--;
        }
        return index;
    }

    public void deleteFromGrid(Tuple tuple) {
        int index = getIndexInGrid(tuple.getEntries());
        //find the range
        deleteFromRange(index, tuple.getClusteringKey());
    }

    private void deleteFromRange(int index, Comparable key) {
        Bucket bucket = deserializeBucket(gridList.elementAt(index));
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
                    gridList.setElementAt(null, index);
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

    private void deleteBucket(String bName) {
        File f = new File("src/main/resources/data/" + bName + ".class");
        f.delete();
    }

    public void insertInGrid(Tuple tuple, String pageName) {
        int index = getIndexInGrid(tuple.getEntries());
        insertInRange(index, tuple.getClusteringKey(), tuple.getEntries(), pageName);
    }

    private void insertInRange(int index, Comparable key, Hashtable<String, Comparable> entries, String pageName) {
        Hashtable<String, Comparable> ht = new Hashtable<>();
        for (String s : columnNames) {
            if (entries.get(s) != null)
                ht.put(s, entries.get(s));
        }
        BucketEntry newEntry = new BucketEntry(key, pageName, ht);
        //check if bucket exist:
        if (gridList.elementAt(index) == null) {
            String bucketName = getnewBucketName(tableName);
            Bucket bucket = new Bucket(bucketName);
            bucket.bucketBody.addElement(newEntry);
            gridList.setElementAt(bucketName, index);
            serializeBucket(gridList.elementAt(index), bucket);
        } else {
            Bucket currentBucket = deserializeBucket(gridList.elementAt(index));
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
            serializeBucket(gridList.elementAt(index), currentBucket);
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
        int index = getIndexInGrid(tuple.getEntries());
        Bucket bucket = deserializeBucket(gridList.elementAt(index));
        while (bucket.bucketBody.size() == DBApp.B && bucket.bucketBody.lastElement().getClusteringKey().compareTo(tuple.getClusteringKey()) < 0) {
            bucket = deserializeBucket(bucket.overFlow);
        }
        bucket.bucketBody.elementAt(getBucketEntryIndex(bucket, tuple.getClusteringKey())).setPageName(pageName);
        serializeBucket(bucket.getBucketname(), bucket);
    }

    private int getBucketEntryIndex(Bucket bucket, Comparable clusteringKey) {
        //index of Clustering key in the bucket
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

    private BucketEntry getBucketEntry(int index, Comparable key) {
        Bucket bucket = deserializeBucket(gridList.elementAt(index));
        while (bucket.bucketBody.size() == DBApp.B && bucket.bucketBody.lastElement().getClusteringKey().compareTo(key) < 0) {
            bucket = deserializeBucket(bucket.overFlow);
        }
        return bucket.bucketBody.elementAt(getBucketEntryIndex(bucket, key));
    }

    public void changeBucket(Tuple old, Tuple neo, String pagename) {
        int oldindex = getIndexInGrid(old.getEntries());
        int neoindex = getIndexInGrid(neo.getEntries());
        if (oldindex != neoindex) {
            deleteFromRange(oldindex, old.getClusteringKey());
            insertInRange(neoindex, neo.getClusteringKey(), neo.getEntries(), pagename);
        } else {
            BucketEntry be = getBucketEntry(oldindex, old.getClusteringKey());
            for (String s : columnNames) {
                be.getColumnvalues().put(s, neo.getEntries().get(s));
            }
        }
    }

    public Vector<String> getNeededPageNamesDelete(Hashtable<String, Object> colNameValues) {
        Hashtable<String, Comparable> ht = new Hashtable<String, Comparable>();
        HashSet<String> pageNames = new HashSet<>();
        for (String s : colNameValues.keySet()) {
            ht.put(s, (Comparable) colNameValues.get(s));
        }
        int index = getIndexInGrid(ht);
        String bucketName = gridList.get(index);
        while (bucketName != null) {
            Bucket b = deserializeBucket(bucketName);
            r:
            for (BucketEntry be : b.bucketBody) {
                for (String s : ht.keySet()) {
                    if (ht.get(s).compareTo(be.getColumnvalues().get(s)) != 0)
                        continue r;
                }
                pageNames.add(be.getPageName());
            }
            bucketName = b.overFlow;
        }
        Vector<String> output = new Vector<>();
        for (String s : pageNames)
            output.addElement(s);
        return output;

    }

    public String getPageNameFromIndex(Comparable clusteringKey) {
        Hashtable<String, Comparable> ht = new Hashtable<>();
        ht.put(pk, clusteringKey);
        int index = getIndexInGrid(ht);
        if (gridList.elementAt(index) != null) {
            Bucket bucket = deserializeBucket(gridList.elementAt(index));
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
                    String p = getLargestOfSmaller(index);
                    if (p != null) {
                        return p;
                    } else {
                        return bucket.bucketBody.firstElement().getPageName();
                    }
                }
            }
        } else {
            String p = getLargestOfSmaller(index);
            if (p != null) {
                return p;
            } else {
                return getSmallestOfLarger(index);
            }
        }
    }

    private String getLargestOfSmaller(int index) {
        if (index < 0) return null;
        if (null == gridList.elementAt(index)) {
            getLargestOfSmaller(index - 1);
        } else {
            Bucket bucket = deserializeBucket(gridList.elementAt(index));
            while (!bucket.overFlow.equals("")) {
                bucket = deserializeBucket(bucket.overFlow);
            }
            return bucket.bucketBody.lastElement().getPageName();
        }
        return null;
    }

    private String getSmallestOfLarger(int indexOfRange) {
        if (indexOfRange > gridList.size()) return null;
        if (null == gridList.elementAt(indexOfRange)) {
            getSmallestOfLarger(indexOfRange + 1);
        } else {
            Bucket bucket = deserializeBucket(gridList.elementAt(indexOfRange));
            return bucket.bucketBody.firstElement().getPageName();
        }
        return null;

    }

    public Vector<String> getNeededPageNames(String tableName, String colName, Comparable colValue, String Operator) {
        Vector<String> output = new Vector<>();
        int colIndex = columnNames.indexOf(colName);
        Vector<Comparable> ranges = columnRanges.elementAt(colIndex);
        int indexOfRange = indexOfRange(ranges, colValue);
        int order = columnNames.size() - colIndex - 1;
        HashSet<String> pageNames = new HashSet<>();
        int index = 0;
        switch (Operator) {
            case "=":
                index = indexOfRange * (int) Math.pow(11, order);
                while (index < gridList.size()) {
                    for (int i = index; i < index + (int) Math.pow(11, order); i++) {
                        getEqualValue(colName, colValue, pageNames, gridList.elementAt(i));
                    }
                    index += Math.pow(11, order + 1);
                }
                break;
            case "<":
                if (ranges.elementAt(indexOfRange).compareTo(colValue) > 0)
                    indexOfRange--;
                while (indexOfRange >= 0) {
                    index = indexOfRange * (int) Math.pow(11, order);
                    while (index < gridList.size()) {
                        for (int i = index; i < index + (int) Math.pow(11, order); i++) {
                            getLessValue(colName, colValue, pageNames, gridList.elementAt(i));
                        }
                        index += Math.pow(11, order + 1);
                    }
                    indexOfRange--;
                }
                break;
            case ">":
                while (indexOfRange < 10) {
                    index = indexOfRange * (int) Math.pow(11, order);
                    while (index < gridList.size()) {
                        for (int i = index; i < index + (int) Math.pow(11, order); i++) {
                            getmoreValue(colName, colValue, pageNames, gridList.elementAt(i));
                        }
                        index += Math.pow(11, order + 1);
                    }
                    indexOfRange++;
                }
                break;
            case "<=":
                while (indexOfRange < 10) {
                    index = indexOfRange * (int) Math.pow(11, order);
                    while (index < gridList.size()) {
                        for (int i = index; i < index + (int) Math.pow(11, order); i++) {
                            getLessEqualValue(colName, colValue, pageNames, gridList.elementAt(i));
                        }
                        index += Math.pow(11, order + 1);
                    }
                    indexOfRange++;
                }
                break;
            case ">=":
                while (indexOfRange < 10) {
                    index = indexOfRange * (int) Math.pow(11, order);
                    while (index < gridList.size()) {
                        for (int i = index; i < index + (int) Math.pow(11, order); i++) {
                            getmoreEqualValue(colName, colValue, pageNames, gridList.elementAt(i));
                        }
                        index += Math.pow(11, order + 1);
                    }
                    indexOfRange++;
                }
                break;
        }
        for (String s : pageNames)
            output.addElement(s);
        return output;
    }

    private void getLessValue(String colName, Comparable colValue, HashSet<String> pageNames, String bucketName) {
        if (bucketName != null) {
            Bucket bucket = deserializeBucket(bucketName);
            for (BucketEntry be : bucket.bucketBody) {
                if (be.getColumnvalues().get(colName).compareTo(colValue) < 0) {
                    pageNames.add(be.getPageName());
                }
            }
            getLessValue(colName, colValue, pageNames, bucket.overFlow);
        }
    }

    private void getmoreValue(String colName, Comparable colValue, HashSet<String> pageNames, String bucketName) {
        if (bucketName != null) {
            Bucket bucket = deserializeBucket(bucketName);
            for (BucketEntry be : bucket.bucketBody) {
                if (be.getColumnvalues().get(colName).compareTo(colValue) > 0) {
                    pageNames.add(be.getPageName());
                }
            }
            getmoreValue(colName, colValue, pageNames, bucket.overFlow);
        }
    }

    private void getLessEqualValue(String colName, Comparable colValue, HashSet<String> pageNames, String bucketName) {
        if (bucketName != null) {
            Bucket bucket = deserializeBucket(bucketName);
            for (BucketEntry be : bucket.bucketBody) {
                if (be.getColumnvalues().get(colName).compareTo(colValue) <= 0) {
                    pageNames.add(be.getPageName());
                }
            }
            getLessEqualValue(colName, colValue, pageNames, bucket.overFlow);
        }
    }

    private void getmoreEqualValue(String colName, Comparable colValue, HashSet<String> pageNames, String bucketName) {
        if (bucketName != null) {
            Bucket bucket = deserializeBucket(bucketName);
            for (BucketEntry be : bucket.bucketBody) {
                if (be.getColumnvalues().get(colName).compareTo(colValue) >= 0) {
                    pageNames.add(be.getPageName());
                }
            }
            getEqualValue(colName, colValue, pageNames, bucket.overFlow);
        }
    }

    private void getEqualValue(String colName, Comparable colValue, HashSet<String> pageNames, String bucketName) {
        if (bucketName != null) {
            Bucket bucket = deserializeBucket(bucketName);
            for (BucketEntry be : bucket.bucketBody) {
                if (be.getColumnvalues().get(colName).compareTo(colValue) == 0) {
                    pageNames.add(be.getPageName());
                }
            }
            getEqualValue(colName, colValue, pageNames, bucket.overFlow);
        }
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
}

