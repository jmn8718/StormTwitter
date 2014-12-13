package app.twitter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.TwitterApi;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;

public class TwitterApp {
	String STREAM_API_URL_FILTER = "https://stream.twitter.com/1.1/statuses/filter.json";
	OAuthService service;
	Token accessToken;
	BufferedReader reader;
	int mode;
	String file,filter;
	public TwitterApp(int mode, String file, String userKey, String userSecret, String token,
			String tokenSecret) {
		this.mode = mode;
		this.file = file;
		this.service = new ServiceBuilder().provider(TwitterApi.class)
				.apiKey(userKey).apiSecret(userSecret).build();
		this.accessToken = new Token(token, tokenSecret);
	}	
		
	public void connect(String csvFilter) throws FileNotFoundException {
		this.filter = csvFilter;
		if (this.mode == 1)
			this.connectFile();
		else if (this.mode == 2)
			this.connectLive();
	}
	private void connectLive() {
		System.out.println("Connecting to TWITTER STREAM API...");
		OAuthRequest request = new OAuthRequest(Verb.POST,
				STREAM_API_URL_FILTER);
		request.addHeader("version", "HTTP/1.1");
		request.addHeader("host", "stream.twitter.com");
		request.setConnectionKeepAlive(true);
		request.addHeader("user-agent", "Twitter Stream Reader");
		request.addBodyParameter("track", this.filter);
		service.signRequest(accessToken, request);
		Response response = request.send();

		reader = new BufferedReader(new InputStreamReader(response.getStream()));
		System.out.println("Connection success");		
	}


	private void connectFile() {
		System.out.println("Connecting to TWITTER FILE...");
		try {
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(this.file)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		System.out.println("Connection success");
	}

	private String getTweet(){
		String resul = null;
		if(this.mode==1)
			resul = this.getTweetFile();
		else if (this.mode==2)
			resul = this.getTweetLive();
		return resul;
	}
	private String getTweetFile(){
		String line = null;
		try {
			line = reader.readLine();
			if(line==null){
				System.out.println("*/***************///////-----EOF");
				line = "{\"EOF\":\"si\"}";
				try {
					TimeUnit.SECONDS.sleep(5);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				//this.disconnect();
			}
		} catch (IOException e) {
			System.out.println("*************------ERROR getTweetFILE");
		}
		return line;
	}
	private String getTweetLive(){
		String line;
		try {
			line = reader.readLine();
			while (line == null || line.length() <= 0) {
				line = reader.readLine();
				System.out.print("+");
			}
		} catch (IOException e) {
			//e.printStackTrace();
			System.out.println("*************------ERROR getTweetLIVE");
			this.disconnect();
			this.connectLive();
			line = "{\"error\":\"si\"}";
		}
		return line;
	}
	
	public void disconnect(){
		System.out.println("Disconecting from TWITTER...");
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Disconnection success");
	}

	public static void main(String[] args) throws IOException {
		String dirIP = args[0];
		int port = Integer.parseInt(args[1]);
		int mode = Integer.parseInt(args[2]);
		String file = args[3];
		String userKey = args[4];
		String userSecret = args[5];
		String token = args[6];
		String tokenSecret = args[7];

		TwitterApp app = new TwitterApp(mode, file, userKey, userSecret,
				token, tokenSecret);
		
		String filtro = "madrid, barcelona, uefa, champions, android, pc, ps, xbox";
		app.connect(filtro);

		InetAddress dirServer = InetAddress.getByName(dirIP);
		ServerSocket server = new ServerSocket(port, 25, dirServer );
		System.out.println("Server running on "+dirServer.getHostAddress().toString()+":"+port+"...");
		String tweet = new String();
		do{
			Socket socket = server.accept();			
			OutputStream outputStream = socket.getOutputStream();
			OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);			
			BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);
			do {
				tweet = app.getTweet(); System.out.print("·");
			} while(tweet==null);	
			if(tweet.length()>17){
				System.out.println(tweet.substring(tweet.length()-15,tweet.length()-5)+"   "+tweet.substring(tweet.length()-5));
			} else System.out.println("--------------"+tweet);
			bufferedWriter.write(tweet);
			bufferedWriter.flush();					
			socket.close();
		} while(true);
	}

}
