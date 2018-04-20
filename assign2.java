package assignment2;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class assign2 {

    public static void main(String args[]) {

//        input.put("file1.txt", "foo foo bar cat dog dog");
//        input.put("file2.txt", "foo house cat cat dog");
//        input.put("file3.txt", "foo foo foo bird");

        int numThreads = 0;

        try{
            numThreads = Integer.parseInt(args[0]);
        }catch (IllegalArgumentException illegalArgumentException){
            System.err.println("The number of threads must be a whole positive number");
            illegalArgumentException.printStackTrace();
        }

        int count = args.length;


        Map<String, String> input = new HashMap<String, String>();

        List<String> fileNames = new ArrayList<>();
        for(int i = 1; i < count; i++){
            fileNames.add(args[i]);
        }

        for(String name: fileNames){
            System.out.println(name);
        }

        try {

            for (int j = 0; j < fileNames.size(); j++) {
                BufferedReader br = new BufferedReader(new FileReader(fileNames.get(j)));
                    StringBuilder sb = new StringBuilder();
                    String line = br.readLine();

                    while (line != null) {
                        sb.append(line);
                        sb.append(System.lineSeparator());
                        line = br.readLine();
                    }
                    input.put(fileNames.get(j), sb.toString());
            }
        } catch (FileNotFoundException fileNotFoundException){
            System.err.println("File not found");
            fileNotFoundException.printStackTrace();
        } catch (IOException ioException){
            System.err.println("IO Exception");
            ioException.printStackTrace();
        }

        // APPROACH #3: Distributed MapReduce
        {
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:

            final List<assign2.MappedItem> mappedItems = new LinkedList<assign2.MappedItem>();

            final assign2.MapCallback<String, assign2.MappedItem> mapCallback = new assign2.MapCallback<String, assign2.MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<assign2.MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            List<Thread> mapCluster = new ArrayList<Thread>(input.size());

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while (inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                final String file = entry.getKey();
                final String contents = entry.getValue();

                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        map(file, contents, mapCallback);
                    }
                });
                mapCluster.add(t);
                t.start();
            }

            // wait for mapping phase to be over:
            for (Thread t : mapCluster) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            // GROUP:
            long startTime = System.nanoTime();

            ExecutorService fixedPool = Executors.newFixedThreadPool(numThreads);

            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<assign2.MappedItem> mappedIter = mappedItems.iterator();
            while (mappedIter.hasNext()) {
                assign2.MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }
            long stopTime = System.nanoTime();
            System.out.println(stopTime - startTime + " nanoseconds");

            // REDUCE:

            final assign2.ReduceCallback<String, String, Integer> reduceCallback = new assign2.ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };

            List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while (groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                final String word = entry.getKey();
                final List<String> list = entry.getValue();

                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        reduce(word, list, reduceCallback);
                    }
                });
                reduceCluster.add(t);
                t.start();
            }

            // wait for reducing phase to be over:
            for (Thread t : reduceCluster) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            System.out.println(output);
        }
    }

    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, assign2.MapCallback<String, assign2.MappedItem> callback) {
        String[] words = contents.replaceAll("[^a-zA-Z ]", " ").toLowerCase().split("\\s+");
        List<assign2.MappedItem> results = new ArrayList<assign2.MappedItem>(words.length);
        for (String word : words) {
            results.add(new assign2.MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

//    public  static interface GroupCallback<E, K, V> {
//        public void groupDone(E e, Map<K, V> results);
//    }

    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K, V> results);
    }

    public static void reduce(String word, List<String> list, assign2.ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for (String file : list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }


}
