package test;

import tukano.api.Result;
import tukano.api.User;
import tukano.clients.rest.RestBlobsClient;
import tukano.clients.rest.RestShortsClient;
import tukano.clients.rest.RestUsersClient;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

public class Test {
	
	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s");
	}
	
	public static void main(String[] args ) throws Exception {
		/*new Thread( () -> {
			try { 
				TukanoRestServer.main( new String[] {} );
			} catch( Exception x ) {
				x.printStackTrace();
			}
		}).start();*/

		Thread.sleep(1000);
		
		//var serverURI = String.format("http://localhost:%s/rest", TukanoRestServer.PORT);
		var serverURI = "http://127.0.0.1:8080/project2_SCC/rest/";
		var uriLogin = "http://127.0.0.1:8080/project2_SCC/rest/login";
		String blobEndpoint = "http://127.0.0.1:8080/project2_SCC/rest/blobs";
		//var serverURI = "https://project1scc24256018360431.azurewebsites.net/rest";

		var blobs = new RestBlobsClient(serverURI);
		var users = new RestUsersClient( serverURI);
		var shorts = new RestShortsClient(serverURI);

		// Step 1: Login and Retrieve Session Cookie
		HttpClient client = HttpClient.newHttpClient();
		String form = "username=liskov&password=54321";
		HttpRequest loginRequest = HttpRequest.newBuilder()
				.uri(URI.create(uriLogin))
				.header("Content-Type", "application/x-www-form-urlencoded")
				.POST(HttpRequest.BodyPublishers.ofString(form))
				.build();

		HttpResponse<String> loginResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());

		// Extract cookie
		List<String> cookies = loginResponse.headers().allValues("Set-Cookie");
		if (cookies.isEmpty()) {
			System.out.println("No cookies received. Login failed.");
			return;
		}
		String sessionCookie = cookies.get(0).split(";")[0];
		System.out.println("Session Cookie: " + sessionCookie);

		Result<tukano.api.Short> s1, s2;

		show(users.createUser( new User("liskov", "54321", "liskov@mit.edu", "Barbara Liskov") ));

		show(s2 = shorts.createShort("liskov", "54321"));

		var blobUrl = URI.create(s2.value().getBlobUrl());
		System.out.println( "------->" + blobUrl );

		var blobId = new File( blobUrl.getPath() ).getName();
		System.out.println( "BlobID:" + blobId );

		var token = blobUrl.getQuery().split("=")[1];
		System.out.println("Token: " + token);

		uploadBlob(client, blobEndpoint, blobId, randomBytes( 100 ), sessionCookie, token);

		System.exit(0);
	}
	
	
	private static Result<?> show( Result<?> res ) {
		if( res.isOK() )
			System.err.println("OK: " + res.value() );
		else
			System.err.println("ERROR:" + res.error());
		return res;
		
	}
	
	private static byte[] randomBytes(int size) {
		var r = new Random(1L);

		var bb = ByteBuffer.allocate(size);
		
		r.ints(size).forEach( i -> bb.put( (byte)(i & 0xFF)));		

		return bb.array();
		
	}

	private static void uploadBlob(HttpClient client, String blobEndpoint, String blobId, byte[] data, String sessionCookie, String token) throws Exception {
		String uploadUrl = blobEndpoint + "/" + blobId + "?token=" + token;

		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(uploadUrl))
				.header("Content-Type", "application/octet-stream")
				.header("Cookie", sessionCookie)
				.POST(HttpRequest.BodyPublishers.ofByteArray(data))
				.build();

		HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
		System.out.println("Upload Response: " + response.statusCode() + " - " + response.body());
	}


}
