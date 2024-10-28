package storageConnections;

import com.azure.cosmos.*;

public class AzureCosmosDB {

    private static CosmosContainer instance;

    public synchronized static CosmosContainer getContainer(String containerName) {
        if( instance != null)
            return instance;

        CosmosClient client = new CosmosClientBuilder()
                .endpoint(System.getProperty("COSMOSDB_URL"))
                .key(System.getProperty("COSMOSDB_KEY"))
                //.directMode()
                .gatewayMode()
                // replace by .directMode() for better performance
                .consistencyLevel(ConsistencyLevel.SESSION)
                .connectionSharingAcrossClientsEnabled(true)
                .contentResponseOnWriteEnabled(true)
                .buildClient();

        CosmosDatabase db = client.getDatabase(System.getProperty("COSMOSDB_DATABASE"));
        instance = db.getContainer(containerName);

        return instance;
    }

}
