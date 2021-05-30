public class BucketEntry implements Comparable {
    private String pageName;
    private Comparable clusteringKey;

    public void setPageName(String pageName) {
        this.pageName = pageName;
    }

    public BucketEntry(Comparable clusteringKey, String pageName) {
        this.pageName = pageName;
        this.clusteringKey = clusteringKey;
    }

    public String getPageName() {
        return pageName;
    }

    public Comparable getClusteringKey() {
        return clusteringKey;
    }


    @Override
    public int compareTo(Object o) {
        return clusteringKey.compareTo((BucketEntry)o);
    }
}
