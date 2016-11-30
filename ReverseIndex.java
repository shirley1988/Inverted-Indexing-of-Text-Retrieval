import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ReverseIndex {
 
     public static class Map extends MapReduceBase implements Mapper<LongWritable, 
                        Text, Text, Text> {       
        JobConf conf;
        int argc;          // number of keywords
        private static Text[] keywords;     // array to store keywords
        private IntWritable[] num;  // array to store the number of each keyword
        
        public void configure( JobConf job) {
            this.conf = job;
            // retrieve #keywords from JobConf
            this.argc = Integer.parseInt( conf.get("argc") ); 
            this.keywords = new Text[this.argc];   // initializa keyword array
            this.num = new IntWritable[this.argc]; // initialize array of number
            
            
        }
        public void map(LongWritable docId, Text value, OutputCollector<Text,
                        Text> output, Reporter reporter) throws IOException {
             // retrieve keyword from JobConf
             for (int i = 0; i < this.argc; i++) {
                this.keywords[i] = new Text( this.conf.get( "keyword" + i ) );
                this.num[i] = new IntWritable(0);
             }
             // get the current file name
             FileSplit fileSplit = ( FileSplit )reporter.getInputSplit();
             String filename = "" + fileSplit.getPath().getName();        
             String line = value.toString();
             
             // tokenize the value. 
             StringTokenizer tokenizer = new StringTokenizer(line);
             Text word = new Text();
             while ( tokenizer.hasMoreTokens() ) {       // has next tokenized String
                 word.set( tokenizer.nextToken() );      // store it into word
                 // check if word is one of the keywords
                 for (int i = 0; i < this.argc; i++ ) {
                     // if word is one of the keywords, the number of apparence 
                     // stored in array num need to incease by 1.
                     // if ( (keywords[i].toString()).equals( word.toString()) )  {
                        if ( word.toString().contains( (keywords[i].toString())) ) {
                          int tmp = num[i].get();
                          ++tmp;
                          num[i].set(tmp);
                      }
                 }
             }   
             // process and collect the results to output in a given format.
             // filename and number of apparence is parse by a ','.
             for ( int i = 0; i < this.argc; i++ )  {
                 Text filename_count = new Text(filename + "," + num[i].get() );
                 output.collect(keywords[i], filename_count);
             }
        }
    } 
    
    public static class Reduce extends MapReduceBase implements Reducer<Text, 
                       Text, Text, Text> {
                       
        public void reduce( Text key, Iterator<Text> values, OutputCollector<
            Text, Text> output, Reporter reporter) throws IOException {
               
            Text filename_count = new Text(); // store current filename_account
            // hashtable to maintain all filename_accounts.
            Hashtable<String, String> docList = new Hashtable<String, String>();
            // if not the end of file for a given key
            while ( values.hasNext() ) {
                   filename_count = values.next(); 
                   // parse filename_count into a string array
                   String [] copy = (filename_count.toString()).split(",");
                   // check if the filename has appears in hashtable
                   if ( docList.containsKey(copy[0]) ) {
                       // increase the value of the filename
                       int count = Integer.parseInt(docList.get(copy[0]));
                       count += Integer.parseInt(copy[1]);
                       docList.put( copy[0], count + "" );
                    } else {
                       // insert a new item to hashtable
                       docList.put( copy[0], copy[1] );
                    }
               }
               
               // this module implements reverse indexing basic functionality.
               // if the keywords exits in a file, output it.
               /*
               Enumeration posting = docList.keys();
               while (posting.hasMoreElements()) {
                   String filename = "" + posting.nextElement();
                   Text list_ele = new Text(filename + "," + docList.get(filename));
                   if ( Integer.parseInt(docList.get(filename)) > 0 ) {
                       output.collect(key, list_ele);
                   }
               } 
               */
               // this module implements basic together with sorting the filenames
               // accoding to its number of appearance.
               Enumeration posting = docList.keys();    // iterator of hashtable
               // an arraylist is needed to do the sorting
               List<String> al = new ArrayList<String>();  
               // add all items in hashtable to arraylist.
               while (posting.hasMoreElements()) {
                   String filename = "" + posting.nextElement();
                   al.add(filename + "," + docList.get(filename));
               }   
               // sort the arraylist
               Collections.sort(al, new Comparator<String>() {
                   public int compare(String s1, String s2)  {
                       String [] pair1 = s1.split(",");
                       String [] pair2 = s2.split(",");
                       return Integer.parseInt(pair1[1]) < Integer.parseInt(pair2[1]) ? 1 : -1;
                   }
               });
               // collect the the filename_count if count is greater than 0.
               for (int i = 0; i < al.size(); i++) {
                   String [] pair = al.get(i).split(",");
                   if (Integer.parseInt(pair[1]) > 0) {
                       output.collect(key, new Text(al.get(i)));
                   }
               }
        }
    }
    
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(ReverseIndex.class);
        conf.setJobName("ReverseIndex");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        long start = System.currentTimeMillis();  // start the timer
        // input directory name
        FileInputFormat.setInputPaths(conf, new Path(args[0])); 
        // output directory name
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        System.out.println(" starting to set argc ");
        // argc maintains #keywords
        conf.set( "argc", String.valueOf( args.length - 2) );
        for ( int i = 0; i < args.length - 2; i++ ) {
             // keyword1, keyword2, ...
             conf.set( "keyword" + i, args[i + 2] );
        }
        System.out.println(" start to run job" );
        JobClient.runJob(conf);
        long end = System.currentTimeMillis(); // finish the timer
        // print the execution time
        System.out.println("execution time: " + (end - start)); 
    }
}