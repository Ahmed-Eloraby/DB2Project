import java.io.*;
import java.util.Properties;
import java.util.Vector;

public class Table implements Serializable {
    String name;
    int i;

    public Table(String name) {
        this.name = name;
        i = 1;
    }

    public void createPage() throws IOException {
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        InputStream is = new FileInputStream(fileName);

            prop.load(is);

        int N =Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
        try {
            Vector<Object> page = new Vector<Object>();
            page.setSize(N);
            ObjectOutputStream o = new
                    ObjectOutputStream(
                    new FileOutputStream("src/main/Pages/" +name + i + ".bin"));
            i++;
            o.writeObject(page);
            o.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public static void main(String[] args) throws IOException {

    }
}
