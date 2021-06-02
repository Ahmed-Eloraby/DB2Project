public class BucketEntry implements Comparable {
    private String pageName;
    private Comparable clusteringKey,columnValue;

    public void setPageName(String pageName) {
        this.pageName = pageName;
    }

    public BucketEntry(Comparable clusteringKey, String pageName,Comparable columnValue) {
        this.pageName = pageName;
        this.clusteringKey = clusteringKey;
        this.columnValue = columnValue;
    }

    public Comparable getColumnValue() {
        return columnValue;
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
