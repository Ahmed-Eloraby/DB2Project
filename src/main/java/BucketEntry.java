public class BucketEntry {
    private String pageName;
    private int pageIndex, recordIndex;

    public BucketEntry(String pageName, int recordIndex, int pageIndex) {
        this.pageName = pageName;
        this.recordIndex = recordIndex;
        this.pageIndex = pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
    }

    public int getPageIndex() {
        return pageIndex;
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
