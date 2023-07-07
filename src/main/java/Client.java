import java.io.IOException;

public class Client {
    public static void main(String[] args) throws IOException {
        HbaseCRUD hbaseCRUD = new HbaseCRUD();
        Task task = new Task();
        //task.firstTask(hbaseCRUD);
        task.secondTask(hbaseCRUD);


    }
}