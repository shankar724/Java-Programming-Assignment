import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Query{
    public static int queryCounter = 0;
    public int queryId;
    public ArrayList<String> keywordsList;      // [keywords] 
    public Map<String, ArrayList<String>> queryResponse;    // Keyword: [{Line_i, File_i}]
    public String queryStatus;     // PENDING/SUCCESS 

    Query(ArrayList<String> keywords){
        this.queryId = queryCounter++;
        this.keywordsList = keywords;
        this.queryStatus = "PENDING";
        // queryResponse = new Map<String, new List<String>>();
    }

    synchronized void updateResponse(){
        
    }
}

// Different worker threads will pick one of the keywords and a set of files and search the file and update the ouptut at the last, once search is completed. 
class ImplementSearch implements Runnable{

    @Override
    public void run() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'run'");
    }
    
}

// One thread has the responsibility for looking at the query input and distributing the search work across other threadpool/
class SearchQuery implements Runnable{
    Query query;

    SearchQuery(Query query){
        this.query = query;
    }

    @Override
    public void run() {
        
    }
    
}


public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        ArrayList<Query> queryList = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        // Read all files and store as strings in some global or static variables. 
        while(true){
            System.out.println("Please select the *option number* for the desired query.");
            System.out.println("1: New Query Input \n 2: Check Query Response \n 3: Exit");
            int option = sc.nextInt();
            if(option == 1){
                System.out.println("Enter the keywords (space-separated) to be searched across the set of files. ");
                String str = sc.nextLine();
                String[] keywordsArray = str.split("\\s+");
                // Create an ArrayList to store the words
                ArrayList<String> keywordsList = new ArrayList<>();
                for (String word : keywordsArray) {
                    keywordsList.add(word);
                }
                Query newQuery = new Query(keywordsList);
                queryList.add(newQuery);
                executorService.execute(new SearchQuery(newQuery));
            } else if(option == 2){
                System.out.println("Enter the query id.");
                int queryId = sc.nextInt();
                // Invoke the method and pass it to the thread for checking if the response exists.
                if(queryId < 0 || queryId >= queryList.size()){
                    System.out.println("Invalid query ID.");
                } else{
                    Query queryObj = queryList.get(queryId);
                    if(queryObj.queryStatus.equals("SUCCESS")){
                        for (String key : queryObj.queryResponse.keySet()) {
                            ArrayList<String> values = queryObj.queryResponse.get(key);
                            System.out.print(key + ": [");
                            for (int i = 0; i < values.size(); i++) {
                                System.out.print(values.get(i));
                                if (i < values.size() - 1) {
                                    System.out.print(", ");
                                }
                            }
                            System.out.println("]");
                        }
                    } else{
                        System.out.println("Query Execution is IN PROGRESS");
                    }
                }
            } else if(option == 3){
                System.out.println("Thanks for checking the program. \n Terminating the program.");
                break;
            } else{
                System.out.println("Invalid Input");
            }


        }
    }
}

/*
Objective:
1) A set of files which we have to read and store.
2) User Defined Program: a) Query b) Get Response 3) Exit
3) Query: input [keywords] ; Output: queryId
4) Response: input: queryId     Output: [keywords: fileId_lineNumber]

Some observations:
1) continuous query input should be available at the user side, main thread should be always free to do this task.


Main Thread:
1) input
2) output

Extra thread:
1) Search in multithreaded manner:
-> For any given keyword, we'll distribute the search across different set of files (alloted across different threads)
 */