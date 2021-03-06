import java.io.Serializable;
import java.util.Vector;

public class Bucket implements Serializable {
    String Bucketname;
    Vector<BucketEntry> bucketBody;
    String overFlow;

    public String getBucketname() {
        return Bucketname;
    }


    public Bucket(String name) {
        Bucketname = name;
        bucketBody = new Vector<BucketEntry>();
        overFlow = "";
    }

    public void insertBucketEntry(BucketEntry newEntry) {
        int lo = 0;
        int hi = bucketBody.size() - 1;
        while (lo <= hi) {
            int i = (lo + hi) / 2;
            if (newEntry.compareTo(bucketBody.elementAt(i)) < 0) {
                hi = i - 1;
            } else {
                lo = i + 1;
            }
        }
        bucketBody.insertElementAt(newEntry, lo);
    }

    public Vector<BucketEntry> getBucketBody() {
        return bucketBody;
    }


    public String getOverFlow() {
        return overFlow;
    }

    public void setOverFlow(String overFlow) {
        this.overFlow = overFlow;
    }

    @Override
    public String toString() {
        return "Bucket{" +
                "Bucketname='" + Bucketname + '\'' +
                ", \n bucketBody=" + bucketBody +
                ", overFlow='" + overFlow + '\'' +
                '}';
    }
}
