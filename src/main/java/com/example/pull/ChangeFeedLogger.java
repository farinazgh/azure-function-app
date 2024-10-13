package com.example.pull;


import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;

public class ChangeFeedLogger {

    private static String storageAccountConnectionString = System.getenv("OtherStorageAccountConnectionString");

    @FunctionName("ChangeFeedLogger")
    public void run(
            @TimerTrigger(name = "timerInfo", schedule = "0 */5 * * * *") String timerInfo,
            final ExecutionContext context) {

        context.getLogger().info("Change Feed Logger function executed at: " + java.time.LocalDateTime.now());

        try {
            // Create a BlobServiceClient to interact with the storage account
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .connectionString(storageAccountConnectionString)
                    .buildClient();

            // The system container for Change Feed
            BlobContainerClient changeFeedContainer = blobServiceClient.getBlobContainerClient("$blobchangefeed");

            // Ensure the container exists before proceeding
            if (changeFeedContainer.exists()) {
                for (BlobItem blobItem : changeFeedContainer.listBlobs()) {
                    context.getLogger().info("Found Change Feed blob: " + blobItem.getName());

                    // Access and process the blob to read its content
                    String blobContent = new String(changeFeedContainer.getBlobClient(blobItem.getName())
                            .downloadContent()
                            .toBytes());

                    // Log the content or do further processing
                    context.getLogger().info("Change Feed Data: " + blobContent);
                }
            } else {
                context.getLogger().warning("Change Feed container does not exist.");
            }
        } catch (Exception ex) {
            context.getLogger().severe("An error occurred: " + ex.getMessage());
        }
    }
}
