import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.PostingsEnum;

import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.*;

public class InvertedIndex {
	static int taatOrCount=0;
	static int taatAndCount=0;
	static int daatAndCount=0;
	static int daatOrCount=0;

	public static void main(String[] args) throws IOException {
		String path_of_index = args[0];
		String outputFileName = args[1];
		String queryFileName = args[2];
		int i=0;
		String sCurrentLine;
		ArrayList<String> inputLines=new ArrayList<String>();
		File input=new File(queryFileName);
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(input),"UTF-8"));
		File out=new File(outputFileName);
		BufferedWriter output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out),"UTF-8"));
		
		while ((sCurrentLine = br.readLine()) != null) {
			inputLines.add(sCurrentLine);
			//System.out.println(inputLines.get(i));
			i++;
		}
		FileSystem fs = FileSystems.getDefault(); //determine the default file system in the host computer
		Path path = fs.getPath(path_of_index);
		IndexReader reader = DirectoryReader.open(FSDirectory.open(path));
		Collection<String> docs = MultiFields.getIndexedFields(reader); //retrieve the fields from the index and store in a collection
		HashMap<String,LinkedList<Integer>> inverted = new HashMap<String,LinkedList<Integer>>();
		//System.out.println(reader.numDocs());
		Iterator<String> itr = docs.iterator(); //Iterator to iterate over the collection containing the fields
		Terms terms;
		TermsEnum abc;
		int count=1;
		PostingsEnum postenum=null;
		ArrayList<String> xyz = new ArrayList<String>();
		while(itr.hasNext()) {
			String element = itr.next();
			if(element.equals("id") || element.equals("_version_"))
			{
				//System.out.println(element+"=123");
				continue;
			}
			//System.out.println(element);
			terms=MultiFields.getTerms(reader,element); //get the terms for a particular field
			abc=terms.iterator(); //Iterator to iterate over the terms and get termEnum
			while(abc.next()!=null)
			{
				xyz.add(abc.term().utf8ToString());
				postenum=MultiFields.getTermDocsEnum(reader,element,abc.term()); //
				LinkedList<Integer> ll = new LinkedList<Integer>();
				while(postenum.nextDoc()!=PostingsEnum.NO_MORE_DOCS)
				{
					ll.add(postenum.docID()); //add the postings of a particular term into its postings list.
				}
				inverted.put(abc.term().utf8ToString(),ll); //storing the postings list of each term in the hashmap
				//System.out.println();
			}
			count=count+1;	  
		}
		for(int j=0;j<inputLines.size();j++)
		{
			String temp=inputLines.get(j);
			taat(temp,inverted,output);
			taatOrCount=0;
			taatAndCount=0;
			daatAnd(temp,inverted,output);
			daatOr(temp,inverted,output);
			daatAndCount=0;
			daatOrCount=0;
			//System.out.println();
		}
		output.close();
		br.close();

	}
	public static void getPostings(String t,HashMap<String,LinkedList<Integer>> index,BufferedWriter output) throws IOException
	{
		LinkedList<Integer> p1=new LinkedList<Integer>(); 
		p1=index.get(t); //Get the posting of the term from the hash map
		output.write("GetPostings\r\n");
		output.write(t+"\r\n");
		output.write("Postings list: ");
		for(int i=0;i<p1.size();i++)
		{
			output.write(p1.get(i)+" ");
		}
		output.write("\r\n");
	}

	public static void taat(String x,HashMap<String,LinkedList<Integer>> index,BufferedWriter output) throws IOException
	{
		LinkedList<Integer> docAndList=new LinkedList<Integer>();
		LinkedList<Integer> docOrList=new LinkedList<Integer>();
		LinkedList<Integer> intermediateAnd=new LinkedList<Integer>();
		LinkedList<Integer> intermediateOr=new LinkedList<Integer>();

		String[] words = x.split("\\s+"); //Split the query into terms of query
		intermediateAnd=index.get(words[0]);
		intermediateOr=index.get(words[0]);
		for(int i=0;i<words.length;i++)
		{
			getPostings(words[i],index,output);
		}
		for(int i=1;i<words.length;i++)
		{
			intermediateAnd=Intersect(intermediateAnd,index.get(words[i]),index); //Intersecting intermediate result with posting of next term and again storing it in intermediate result.
		}
		docAndList=intermediateAnd;
		output.write("TaatAnd\r\n");
		output.write(x+"\r\n");
		output.write("Results: ");
		for(int i=0;i<docAndList.size();i++)
		{
			output.write(docAndList.get(i)+" ");
		}
		if(docAndList.size()==0)
		{
			output.write("empty");
		}
		output.write("\r\nNumber of documents in results: "+docAndList.size()+"\r\n");
		output.write("Number of comparisons: "+taatAndCount+"\r\n");

		for(int i=1;i<words.length;i++)
		{
			intermediateOr=Union(intermediateOr,index.get(words[i]),index); //Union intermediate result with posting of next term and again storing it in intermediate result.
		}
		docOrList=intermediateOr;
		output.write("TaatOr\r\n");
		output.write(x+"\r\n");
		output.write("Results: ");
		for(int i=0;i<docOrList.size();i++)
		{
			output.write(docOrList.get(i)+" ");
		}
		if(docOrList.size()==0)
			output.write("empty");
		output.write("\r\nNumber of documents in results: "+docOrList.size()+"\r\n");
		output.write("Number of comparisons: "+taatOrCount+"\r\n");

		//System.out.println(unionDocList);
	}
	public static LinkedList<Integer> Intersect(LinkedList<Integer> p1,LinkedList<Integer> p2,HashMap<String,LinkedList<Integer>> index)
	{
		//LinkedList<Integer> p1=new LinkedList<Integer>();
		//LinkedList<Integer> p2=new LinkedList<Integer>();
		LinkedList<Integer> answer=new LinkedList<Integer>();
		int a=0,b=0;
		while(!p1.equals(null) && !p2.equals(null) && a<p1.size() && b<p2.size()) //to compare the postings in two lists
		{
			if(p1.get(a).equals(p2.get(b))) //increase the pointers of both lists if same doc_ids and add that doc_id in result and increment the comparison counter
			{
				answer.add(p1.get(a));
				a=a+1;
				b=b+1;
				taatAndCount++; 
			}
			else if(p1.get(a)<p2.get(b)) //increase the pointers of 1st list if it has smaller doc_id as compared 2nd and increment the comparison counter
			{
				a=a+1;
				taatAndCount++;
			}
			else //increase the pointers of 2nd list if it has smaller doc_id as compared to 1st and increment the comparison counter
			{
				b=b+1;
				taatAndCount++;
			}
		}
		return answer;
	}

	public static LinkedList<Integer> Union(LinkedList<Integer> p1,LinkedList<Integer> p2,HashMap<String,LinkedList<Integer>> index)
	{
		//LinkedList<Integer> p1=new LinkedList<Integer>();
		//LinkedList<Integer> p2=new LinkedList<Integer>();
		LinkedList<Integer> answer=new LinkedList<Integer>();
		int a=0,b=0;

		while(!p1.equals(null) && !p2.equals(null) && a<p1.size() && b<p2.size()) //to compare the postings in two lists
		{
			if(p1.get(a).equals(p2.get(b))) //add that doc_id in result and increase the pointers of both lists if same doc_ids and increment the comparison counter
			{
				answer.add(p1.get(a));
				a=a+1;
				b=b+1;
				taatOrCount++;
			}
			else if(p1.get(a)<p2.get(b)) //add the doc_id and increase the pointers of 1st list if it has smaller doc_id as compared 2nd and increment the comparison counter
			{
				answer.add(p1.get(a));
				a=a+1;
				taatOrCount++;
			}
			else //add the doc_id and increase the pointers of 2nd list if it has smaller doc_id as compared 1st and increment the comparison counter
			{
				answer.add(p2.get(b));
				b=b+1;
				taatOrCount++;
			}
		}
		while(a<p1.size()) //add all the remaining doc_ids if 1st list still has elements left
		{
			answer.add(p1.get(a));
			a=a+1;
		}
		while(b<p2.size()) //add all the remaining doc_ids if 2nd list still has elements left
		{
			answer.add(p2.get(b));
			b=b+1;
		}
		return answer;
	}

	public static void daatAnd(String q,HashMap<String,LinkedList<Integer>> index,BufferedWriter output) throws IOException
	{
		ArrayList<LinkedList<Integer>> termsOfQuery=new ArrayList<LinkedList<Integer>>();
		LinkedList<Integer> postings = new LinkedList<Integer>();
		LinkedList<Integer> reference = new LinkedList<Integer>();
		LinkedList<Integer> answer=new LinkedList<Integer>();	
		String[] query=q.split("\\s+");
		int[] postingSize=new int[query.length];
		for(int i=0;i<query.length;i++)
		{
			//System.out.println(query[i]);
			postings=index.get(query[i]);
			termsOfQuery.add(postings);
			//System.out.println(termsOfQuery.get(i));
			postingSize[i]=termsOfQuery.get(i).size();
		}
		int minSize=postingSize[0];
		int minPosting=0;
		//find the postings list which has the minimum size.
		for(int i=1;i<query.length;i++)
		{
			if(postingSize[i]<minSize)
			{
				minSize=postingSize[i];
				minPosting=i;
			}
			//System.out.println(postingSize[i]);
		}
		//System.out.println("Minimum Posting is: "+minPosting+" having value "+minSize);
		reference=termsOfQuery.get(minPosting);
		termsOfQuery.add(0,reference);
		termsOfQuery.remove(minPosting+1);
		//Sort the arraylist of postings list in increasing order of postings list size
		termsOfQuery.sort(new Comparator<LinkedList<Integer>>() {

			@Override
			public int compare(LinkedList<Integer> o1,LinkedList<Integer> o2) {
				if(o1.size()<o2.size())
					return -1;
				else
					return 1;
			}
		});
		/*for(int i=0;i<termsOfQuery.size();i++)
			System.out.println(termsOfQuery.get(i).size());*/
		int count=1;
		int temp_index=0;
		for(int i=0;i<termsOfQuery.get(0).size();i++) //compare the posting list document at a time
		{count=1;
		for(int j=1;j<termsOfQuery.size();j++)
		{
			//temp_index=0;
			int flag=0;
			for(int k=temp_index; k<termsOfQuery.get(j).size();k++)
			{
				daatAndCount++;
				if(termsOfQuery.get(0).get(i).equals(termsOfQuery.get(j).get(k)))
				{
					count++; //if posting found in other document increment counter 
					break;
				}
				else if(termsOfQuery.get(0).get(i)<(termsOfQuery.get(j).get(k))) //skip the rest of the postings
				{
					temp_index=k;
					flag=1;
					break;		
				}
			}
			if(flag==1)
				break; //move to the next term
		}
		if(count==termsOfQuery.size()) //if the counter is equal to number of terms it means all the docs have a posting so add it to result
		{
			answer.add(termsOfQuery.get(0).get(i));
		}
		}
		//System.out.println(answer);
		output.write("DaatAnd\r\n");
		output.write(q+"\r\n");
		output.write("Results: ");
		for(int i=0;i<answer.size();i++)
		{
			output.write(answer.get(i)+" ");
		}
		if(answer.size()==0)
		{
			output.write("empty");
		}
		output.write("\r\nNumber of documents in results: "+answer.size()+"\r\n");
		output.write("Number of comparisons: "+daatAndCount+"\n");
	}

	public static void daatOr(String q,HashMap<String,LinkedList<Integer>> index,BufferedWriter output) throws IOException
	{
		ArrayList<LinkedList<Integer>> termsOfQuery=new ArrayList<LinkedList<Integer>>();
		LinkedList<Integer> postings = new LinkedList<Integer>();
		LinkedList<Integer> reference = new LinkedList<Integer>();
		LinkedList<Integer> answer=new LinkedList<Integer>();
		int comparisons=0;
		int z=0;
		String[] query=q.split("\\s+");
		//find the posting with maximum size and use it as reference so add it to the start of arraylist
		int[] postingSize=new int[query.length];
		for(int i=0;i<query.length;i++)
		{
			//System.out.println(query[i]);
			postings=index.get(query[i]);
			termsOfQuery.add(postings);
			//System.out.println(termsOfQuery.get(i));
			postingSize[i]=termsOfQuery.get(i).size();
		}
		int maxSize=postingSize[0];
		int maxPosting=0;
		for(int i=1;i<query.length;i++)
		{
			if(postingSize[i]>maxSize)
			{
				maxSize=postingSize[i];
				maxPosting=i;
			}
			//System.out.println(postingSize[i]);
		}
		//System.out.println("Minimum Posting is: "+minPosting+" having value "+minSize);
		reference=termsOfQuery.get(maxPosting);
		termsOfQuery.add(0,reference);
		termsOfQuery.remove(maxPosting+1);
		int size=termsOfQuery.size();
		//System.out.println(size);
		int [] Pointers  = new int[size];
		int [] Sizes  = new int[size];
		for(int i = 0 ; i < termsOfQuery.size() ; i++){
			Sizes[i] = termsOfQuery.get(i).size();
			Pointers[i] = 0;
		}
		boolean end=false;
		while(end==false && termsOfQuery.size()!=1) //compare the posting list document at a time
		{
			int counter=0;
			for(int i=0;i<size;i++)
			{
				if (Pointers[i]==Sizes[i])
					counter++;
			}
			if(counter==size)
				end=true;

			if(end==false)
			{
				int j=0;
				while(j<size) 
				{
					if(Pointers[j]<Sizes[j])
					{
						long a=termsOfQuery.get(j).get(Pointers[j]);
						boolean flag=true;
						daatOrCount++;
						for(int i=0;i<answer.size();i++)
						{
							if(a==answer.get(i))//check if the posting is already added to the in the result
							{
								flag=false;
								Pointers[j]++;
							}
						}
						if(flag==true)//if the posting is not there in the result add it to the result.
						{
							answer.add(termsOfQuery.get(j).get(Pointers[j]));
							Pointers[j]++;
						}
					}
					j++;
				}
			}
		}
		while(termsOfQuery.size()==1 && z<termsOfQuery.get(0).size()) //if query of just one term simply add all the postings into the result
		{
			answer.add(termsOfQuery.get(0).get(z));
			z=z+1;
		}
		Collections.sort(answer);
		output.write("DaatOr\r\n");
		output.write(q+"\r\n");
		output.write("Results: ");
		for(int i=0;i<answer.size();i++)
		{
			output.write(answer.get(i)+" ");
		}
		if(answer.size()==0)
		{
			output.write("empty");
		}
		output.write("\r\nNumber of documents in results: "+answer.size()+"\r\n");
		output.write("Number of comparisons: "+daatOrCount+"\n");
		//System.out.println(answer);
	}

}
