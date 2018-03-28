package sk.specta.orsr;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

import javax.annotation.PostConstruct;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrsrParserApplication {

	DateFormat format = new SimpleDateFormat("dd.MM.yyyy", Locale.ENGLISH);
	DecimalFormat decimalFormat;
	
	@PostConstruct
	public void init() throws IOException, ParseException
	{
		RestHighLevelClient client = null;
		try {
			final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY,
			        new UsernamePasswordCredentials("elastic", "testPass"));
			
			HttpHost host = new HttpHost("localhost", 9200, "http");
			RestClientBuilder restCl = RestClient.builder(host).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
	            @Override
	            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
	                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
	            }
	        });
			client = new RestHighLevelClient(restCl);

			
/*			DeleteIndexRequest dreq = new DeleteIndexRequest(getIndexName(currentDayString));
			IndicesClient indices = client.indices();
			if (indices != null)
			{
				DeleteIndexResponse response = client.indices().delete(dreq);
				
			}
*/			
		
			String[] letters = new String [] {"1","2","3", "4","5","6","7","8", "9",
					//
					"a","b","c", "d","e","f","g","h", "i","j",
					"k","l","m", "n","o","p","q","r", "s","t",
					"u","v","x", "y","w","z"
					};
			
			List<String> multiplyLetters = multiplyLetters(letters);
			
			for (String letter : multiplyLetters) {
				List<Company> ret = readWebPageForLetter(letter);
				writeToElastic(client, ret);
			};
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			client.close();
		}
		
	}
	
	private List<String> multiplyLetters(String[] letters) {
		List<String> ret = new ArrayList<String>();
		
		for (String string : letters) {
			for (String string1 : letters) {
				for (String string2 : letters) {
					String rr = string + string1 + string2;
					ret.add(rr);
//					System.out.println(rr);
				}
			}
		}
		
		return ret;
	}

	public List<Company> readWebPageForLetter(String letter) throws IOException, ParseException
	{
		letter +="*";
		List<Company> ret = new ArrayList<Company>();
		
		int pageNum = 1;
		
		boolean finished = true;
		do
		{
			finished = readPerPage(letter, pageNum, ret);
			pageNum++;

			System.out.println(pageNum + " : " + letter);

			try {
				Thread.sleep((long) ((Math.random() * 2000f)));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		while (finished);
		
		return ret;

	}

	private boolean readPerPage(String subject, int pageNum,List<Company> ret)
			throws IOException, ParseException {
		String url = "http://www.orsr.sk/hladaj_subjekt.asp?OBMENO="+subject+"&PF=0&SID=0&S=&R=on&STR="+pageNum;

		boolean finished = false;
		boolean success = true;
		Document document = null;
		do
		{
			try {
				document = Jsoup.connect(url).get();
				success = false;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		while (success);

        String maxPage = document.select("html body table tbody tr td.con").get(1).child(0).text();
        
        String[] splitted = maxPage.split("/");
        
        splitted[0] = splitted[0].trim();
        splitted[1] = splitted[0].trim();
		
		parseDocument(document,ret);
        if (!splitted[0].equals(splitted[1]))
		{
        	finished = true;
		}
        
		
/*		
	    		// INITIALIZE ELASTIC
	    		XContentBuilder builder = jsonBuilder()
	    			    .startObject()
	    			        .field("contractName", contractName)
	    			        .field("contractNumber", contractNumber)
	    			        .field("amount", amount)
	    			        .field("contractor", contractor)
	    			        .field("contractIssuer", contractIssuer)
	    			        .field("date", d)
	    			    .endObject();	
        	
	    		ret.add(builder.string());
	    		
	    		System.out.println(builder.string());
*/
	
	return finished;
	}

	private List<Company> parseDocument(Document document, List<Company> ret) {
		Elements rows = document.select("html body table tbody tr");

		for (Element element : rows) {
			String name = element.select("td div.sbj").text();
			if (name.length() > 0)
			{
				Company c = new Company();
				c.setName(name);
				ret.add(c);
			}
		}
		
		return null;
	}
	
	public void writeToElastic(RestHighLevelClient client, List<Company> items)
	{
		int i = 0;
		for (Company item : items)
		{
			i++;
			IndexRequest indexRequest = new IndexRequest("companyNames", "doc");
			indexRequest.source(item.getName(), XContentType.JSON);
			try {
				client.index(indexRequest);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}

	public static void main(String[] args) throws IOException, ParseException {
		SpringApplication.run(OrsrParserApplication.class, args);

	}

}
