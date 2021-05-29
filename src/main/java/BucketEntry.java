public class BucketEntry implements Comparable {
    private String pageName;
    private Comparable clusteringKey;

    public BucketEntry(Comparable clusteringKey,String pageName) {
        this.pageName = pageName;
        this.clusteringKey = clusteringKey;
    }

    public String getPageName() {
        return pageName;
    }

    public int getRecordIndex() {
        return recordIndex;
    }

    public void setPageName(String pageName) {
        this.pageName = pageName;
    }

    public void setRecordIndex(int recordIndex) {
        this.recordIndex = recordIndex;
    }
}
