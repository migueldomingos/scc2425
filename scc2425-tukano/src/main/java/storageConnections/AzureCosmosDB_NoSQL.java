package storageConnections;

import com.azure.cosmos.*;

public class AzureCosmosDB_NoSQL {

    private static CosmosContainer instance;
    private static final String COSMOSDB_URL = System.getProperty("COSMOSDB_URL");
    private static final String COSMOSDB_KEY = System.getProperty("COSMOSDB_KEY");
    private static final String COSMOSDB_DATABASE = System.getProperty("COSMOSDB_DATABASE");

    public synchronized static CosmosContainer getContainer(String containerName) {
        CosmosClient client = new CosmosClientBuilder()
                .endpoint(COSMOSDB_URL)
                .key(COSMOSDB_KEY)
                .gatewayMode()
                .consistencyLevel(ConsistencyLevel.SESSION)
                .connectionSharingAcrossClientsEnabled(true)
                .contentResponseOnWriteEnabled(true)
                .buildClient();

        CosmosDatabase db = client.getDatabase(COSMOSDB_DATABASE);
        instance = db.getContainer(containerName);

        return instance;
    }
}