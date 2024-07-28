import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Query{
    public static int queryCounter = 0;
    public int queryId;
    public ArrayList<String> keywordsList;      // [keywords] 
    public Map<String, ArrayList<String>> queryResponse;    // Keyword: [{Line_i, File_i}]
    public String queryStatus;     // PENDING/SUCCESS 
    public int totalThreadsAssigned;
    public int threadsWorkPending;

    Query(ArrayList<String> keywords, int threadsUsed){
        this.queryId = queryCounter++;
        this.keywordsList = keywords;
        this.queryStatus = "PENDING";
        queryResponse = new HashMap<>();
        this.threadsWorkPending = threadsUsed; 
        this.totalThreadsAssigned = threadsUsed;
    }

    synchronized void updateResponse(ArrayList<String>keywords, ArrayList<String> response){
        for(String keyword: keywords){
            boolean keywordExistence = queryResponse.containsKey(keyword);
            if(keywordExistence){
                for(String matches: response){
                    queryResponse.get(keyword).add(matches);
                }
            } else{
                // if it doesn't exist
                queryResponse.put(keyword, response);
            }
        }
        this.threadsWorkPending--;
        if(threadsWorkPending <= 0){
            this.queryStatus = "SUCCESS";
        }
    }
}

// Different worker threads will pick one of the keywords and a set of files and search the file and update the ouptut at the last, once search is completed. 
class ImplementSearch implements Runnable{
    Query query;
    ArrayList<String>keywords;
    int startIndex;
    int endIndex;
    ArrayList<ArrayList<String>> filesContent;
    ArrayList<String> Response; 

    ImplementSearch(Query query, ArrayList<String>keywords, int startIndex, int endIndex, ArrayList<ArrayList<String>> filesContent){
        this.query =query;
        this.keywords =keywords;
        this.startIndex= startIndex;
        this.endIndex = endIndex;
        this.filesContent =filesContent;
        this.Response = new ArrayList<>();  
    }

    void keywordSearch(String keyword){
        for(int i = startIndex; i < endIndex; i++){
            // ith file search
            for(int j = 0; j < filesContent.get(i).size(); j++){
                String[] lineWords = filesContent.get(i).get(j).split("\\s+");
                for(String word: lineWords){
                    if(word.equals(keyword)){
                        // keyword found in ith file and jth line
                        String log = "{ Line_" + j + ", File_" + i + "}";
                        Response.add(log);
                        break;      // logging at max 1 match per line. 
                    }
                }
            }
        }
    }
    @Override
    public void run() {
        
        for(String keyword : keywords){
            keywordSearch(keyword);
        }
        // Update the Query object. 
        query.updateResponse(keywords, Response);
    }
    
}

// One thread has the responsibility for looking at the query input and distributing the search work across other threadpool/
class SearchQuery implements Runnable{
    Query query;
    int numFiles;
    int numThreads; 
    ArrayList<ArrayList<String>> filesContent;
    
    SearchQuery(Query query, ArrayList<ArrayList<String>> filesContent, int numThreads){
        this.query = query;
        this.filesContent = filesContent;
        this.numFiles = filesContent.size();
        this.numThreads = numThreads; 
    }

    @Override
    public void run() {
        ExecutorService localExecutorService = Executors.newFixedThreadPool(numThreads);
        ArrayList<String> keywords = query.keywordsList;
        int chunkSize = numFiles/numThreads; 
        // 100 files, threads = 3

        for(int i = 0; i < numThreads; i++){
            int fileStartIndex = i * chunkSize;     // Included
            int fileEndIndex = fileStartIndex + chunkSize;  // Excluded
            if(i==numThreads - 1 ){
                fileEndIndex = numFiles;
            }
            localExecutorService.submit(new ImplementSearch(query, keywords, fileStartIndex, fileEndIndex, filesContent));
        }
        localExecutorService.shutdown();
    }
    
}


public class Main {
    public static void main(String[] args) throws IOException {
        
        // Read the files and store in array list. 
        ArrayList<ArrayList<String>> filesContent = new ArrayList<>();  // <fileid: <fileContent>>
        File dir = new File("output");
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    // Read the content of the file
                    Path filePath = Paths.get(file.getAbsolutePath());
                    List<String> lines = Files.readAllLines(filePath);
                    ArrayList<String> linesContent = new ArrayList<>();
                    for(String str: lines){
                        linesContent.add(str.toLowerCase());
                    }
                    filesContent.add(linesContent);
                }
            }
        }
        // System.out.println(filesContent.size() + " " + filesContent.get(0).size());
        // for(String x: filesContent.get(0)){
        //     System.out.println(x);
        // }

        Scanner sc = new Scanner(System.in);
        ArrayList<Query> queryList = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        int numThreads = 4; // worker threads used for each query search. 

        // Read all files and store as strings in some global or static variables. 
        while(true){
            System.out.println("\n\nPlease select the *option number* for the desired query.");
            System.out.println(" 1: New Query Input \n 2: Check Query Response \n 3: Exit");
            int option = sc.nextInt();
            sc.nextLine();
            if(option == 1){
                System.out.println("Enter the keywords (space-separated) to be searched across the set of files. ");
                String str = sc.nextLine();
                String[] keywordsArray = str.split("\\s+");
                // Create an ArrayList to store the words
                ArrayList<String> keywordsList = new ArrayList<>();
                for (String word : keywordsArray) {
                    if(!keywordsList.contains(word.toLowerCase())){
                        keywordsList.add(word.toLowerCase());
                    }
                    // System.out.println(word.toLowerCase());
                }
                int queryId = queryList.size();
                System.out.println("QueryId Generated: " + queryId);
                Query newQuery = new Query(keywordsList, numThreads);
                queryList.add(newQuery);
                executorService.execute(new SearchQuery(newQuery, filesContent, numThreads));
                
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
                        // System.out.println("Query Execution is IN PROGRESS");
                        int totalThreadsAssigned = queryObj.totalThreadsAssigned;
                        int threadsInProgress = queryObj.threadsWorkPending;
                        System.out.println("Report: " + threadsInProgress +"/" + totalThreadsAssigned + " Threads WORK IN PROGRESS");
                    }
                }
            } else if(option == 3){
                System.out.println("Terminating the program.");
                break;
            } else{
                System.out.println("Invalid Input");
            }
        }
        executorService.shutdown();
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