package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.ErrorCode.FORBIDDEN;
import static tukano.api.Result.ErrorCode.CONFLICT;
import static tukano.api.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.Result.ErrorCode.NOT_FOUND;

import java.util.logging.Logger;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import tukano.api.Blobs;
import tukano.api.Result;
import tukano.impl.rest.TukanoRestServer;
import utils.Hash;
import utils.Hex;

public class JavaBlobs implements Blobs {
	
	private static Blobs instance;
	private static Logger Log = Logger.getLogger(JavaBlobs.class.getName());

	public String baseURI;
	private BlobContainerClient containerClient;
	//private BlobStorage storage;
	
	synchronized public static Blobs getInstance() {
		if( instance == null )
			instance = new JavaBlobs();
		return instance;
	}
	
	private JavaBlobs() {
		//storage = new FilesystemStorage();
		baseURI = String.format("%s/%s/", TukanoRestServer.serverURI, Blobs.NAME);

		containerClient = new BlobContainerClientBuilder()
				.connectionString(System.getProperty("BlobStoreConnection"))
				.containerName(Blobs.NAME)
				.buildClient();
	}
	
	@Override
	public Result<Void> upload(String blobId, byte[] bytes, String token) {
		Log.info(() -> format("upload : blobId = %s, sha256 = %s, token = %s\n", blobId, Hex.of(Hash.sha256(bytes)), token));

		if (!validBlobId(blobId, token))
			return error(FORBIDDEN);

		try {
			BinaryData data = BinaryData.fromBytes(bytes);

			BlobClient blobClient = containerClient.getBlobClient(blobId);

			if (blobClient.exists()) {
				byte[] existingBytes = blobClient.downloadContent().toBytes();

				// Compare the hashes of the existing blob and new bytes
				String existingHash = Hex.of(Hash.sha256(existingBytes));
				String newHash = Hex.of(Hash.sha256(bytes));

				if (existingHash.equals(newHash)) {
					Log.info(() -> format("Blob already exists and matches: blobId = %s", blobId));
					return Result.ok();
				} else {
					Log.warning(() -> format("Conflict: Blob exists but content differs: blobId = %s", blobId));
					return error(CONFLICT);
				}
			} else {
				blobClient.upload(data);
				Log.info(() -> format("New blob uploaded: blobId = %s", blobId));
				return Result.ok();
			}

		} catch (Exception e) {
			e.printStackTrace();
			return error(INTERNAL_ERROR);
		}
	}

	@Override
	public Result<byte[]> download(String blobId, String token) {
		Log.info(() -> format("download : blobId = %s, token=%s\n", blobId, token));

		if( ! validBlobId( blobId, token ) )
			return error(FORBIDDEN);

		try {
			byte[] arr;

			BlobClient blobClient = containerClient.getBlobClient(blobId);

			if (blobClient.exists()) {
				arr = blobClient.downloadContent().toBytes();

				Log.info(() -> format("Blob found and downloaded: blobId = %s, size = %d bytes", blobId, arr.length));
				return Result.ok(arr);
			} else {
				Log.warning(() -> format("Blob not found: blobId = %s", blobId));
				return error(NOT_FOUND);
			}

		} catch (Exception e) {
			e.printStackTrace();
			return error(INTERNAL_ERROR);
		}
	}

	@Override
	public Result<Void> delete(String blobId, String token) {
		Log.info(() -> format("delete : blobId = %s, token=%s\n", blobId, token));

		if (!validBlobId(blobId, token)) {
			return error(FORBIDDEN);
		}

		try {
			BlobClient blob = containerClient.getBlobClient(blobId);

			if (blob.exists()) {
				blob.delete();
				Log.info(() -> format("Blob deleted: %s", blobId));
				return Result.ok();
			} else {
				Log.warning(() -> format("Blob not found: %s", blobId));
				return error(NOT_FOUND);
			}

		} catch (Exception e) {
			e.printStackTrace();
			return error(INTERNAL_ERROR);
		}
	}


	@Override
	public Result<Void> deleteAllBlobs(String userId, String token) {
		Log.info(() -> format("deleteAllBlobs : userId = %s, token=%s\n", userId, token));

		if (!Token.isValid(token, userId)) {
			return error(FORBIDDEN);
		}

		try {
			// Assuming each blob has a userId as part of its metadata or blobId prefix
			PagedIterable<BlobItem> blobs = containerClient.listBlobs();

			for (BlobItem blobItem : blobs) {
				BlobClient blobClient = containerClient.getBlobClient(blobItem.getName());
				blobClient.delete();
				System.out.println("Deleted blob: " + blobItem.getName());
			}

		} catch (Exception e) {
			e.printStackTrace();
			return error(INTERNAL_ERROR);
		}

		return Result.ok();
	}
	
	private boolean validBlobId(String blobId, String token) {		
		return Token.isValid(token, blobId);
	}

	private String toPath(String blobId) {
		return blobId.replace("+", "/");
	}
}
