import java.util.Vector;

public class Bucket {
    Vector<BucketEntry> bucketBody;
    String overFlow;
    public Bucket(){
        bucketBody = new Vector<BucketEntry>();
    }

    public Vector<BucketEntry> getBucketBody() {
        return bucketBody;
    }

    public void setBucketBody(Vector<BucketEntry> bucketBody) {
        this.bucketBody = bucketBody;
    }

    public String getOverFlow() {
        return overFlow;
    }

    public void setOverFlow(String overFlow) {
        this.overFlow = overFlow;
    }
}
