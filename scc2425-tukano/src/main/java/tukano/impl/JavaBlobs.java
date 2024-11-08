package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.ErrorCode.FORBIDDEN;
import static tukano.api.Result.ErrorCode.CONFLICT;
import static tukano.api.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.Result.ErrorCode.NOT_FOUND;
import static tukano.api.Result.ok;

import java.util.List;
import java.util.logging.Logger;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import storageConnections.RedisCache;
import tukano.api.Blobs;
import tukano.api.Result;
import tukano.api.Short;
import tukano.impl.rest.TukanoRestServer;
import utils.Hash;
import utils.Hex;
import utils.JSON;

public class JavaBlobs implements Blobs {
	
	private static Blobs instance;
	private static final Logger Log = Logger.getLogger(JavaBlobs.class.getName());
	private static final String BYTES_CACHE_PREFIX = "bytes:";
	private static final String BlobStoreConnection = System.getProperty("BlobStoreConnection");

	private final BlobContainerClient containerClient;

	synchronized public static Blobs getInstance() {
		if( instance == null )
			instance = new JavaBlobs();
		return instance;
	}
	
	private JavaBlobs() {
		containerClient = new BlobContainerClientBuilder()
				.connectionString(BlobStoreConnection)
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
				cacheBytes(blobId, data);
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

		System.out.println("Passou 1");

		try {

			BinaryData cachedBytes = getCachedBytes(blobId);
			if (cachedBytes != null) {
				return ok(cachedBytes.toBytes());
			}

			System.out.println("Passou 2");

			byte[] arr;

			BlobClient blobClient = containerClient.getBlobClient(blobId);

			System.out.println("Passou 3");

			if (blobClient.exists()) {
				arr = blobClient.downloadContent().toBytes();

				System.out.println("Passou 4");

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
				removeCachedBytes(blobId);
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

		List<String> shorts = JavaShorts.getInstance().getShorts(userId).value();

		if (!Token.isValid(token, userId)) {
			return error(FORBIDDEN);
		}

		for (String s: shorts){

			try {
				BlobClient blob = containerClient.getBlobClient(s);

				if (blob.exists()) {
					blob.delete();
					removeCachedBytes(s);
					Log.info(() -> format("Blob deleted: %s", s));
				}

			} catch (Exception e) {
				e.printStackTrace();
				return error(INTERNAL_ERROR);
			}
		}

		return Result.ok();
	}
	
	private boolean validBlobId(String blobId, String token) {		
		return Token.isValid(token, blobId);
	}

	private String toPath(String blobId) {
		return blobId.replace("+", "/");
	}

	private void cacheBytes(String blobId, BinaryData data){
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			jedis.set(BYTES_CACHE_PREFIX + blobId, JSON.encode(data));
		} catch (JedisException e) {
			Log.warning("Failed to cache the bytes in Redis: " + e.getMessage());
		}
	}

	private BinaryData getCachedBytes(String blobId) {
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			String shortJson = jedis.get(BYTES_CACHE_PREFIX + blobId);
			return shortJson != null ? JSON.decode(shortJson, BinaryData.class) : null;
		} catch (JedisException e) {
			Log.warning("Redis access failed, unable to retrieve cached short.");
			return null;
		}
	}

	private void removeCachedBytes(String blobId) {
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			jedis.del(BYTES_CACHE_PREFIX + blobId);
		} catch (JedisException e) {
			Log.warning("Failed to remove cached short from Redis: " + e.getMessage());
		}
	}
}
