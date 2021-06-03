import java.io.Serializable;
import java.util.Hashtable;

public class BucketEntry implements Comparable, Serializable {
    private String pageName;
    private Comparable clusteringKey;
    private Hashtable<String,Comparable> columnvalues;

    public void setPageName(String pageName) {
        this.pageName = pageName;
    }

    public BucketEntry(Comparable clusteringKey, String pageName,Hashtable<String,Comparable> columnvalues) {
        this.pageName = pageName;
        this.clusteringKey = clusteringKey;
        this.columnvalues = columnvalues;
    }

    public Hashtable<String, Comparable> getColumnvalues() {
        return columnvalues;
    }

    public String getPageName() {
        return pageName;
    }

    public Comparable getClusteringKey() {
        return clusteringKey;
    }


    @Override
    public int compareTo(Object o) {
        return clusteringKey.compareTo(((BucketEntry)o).clusteringKey);
    }

    @Override
    public String toString() {
        return "BucketEntry{" +
                "pageName='" + pageName + '\'' +
                ", clusteringKey=" + clusteringKey +
                ", columnvalues=" + columnvalues +
                '}';
    }
}
