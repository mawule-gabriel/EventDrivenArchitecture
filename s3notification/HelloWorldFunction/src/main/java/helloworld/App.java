package helloworld;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Handler for S3 events that sends notifications via SNS.
 */
public class App implements RequestHandler<S3Event, String> {
    private final SnsClient snsClient;
    private final S3Client s3Client;
    private final String snsTopicArn;
    private final String environment;
    private final String email;

    public App() {
        // Initialize AWS clients
        Region region = Region.of(System.getenv("AWS_REGION"));
        this.snsClient = SnsClient.builder().region(region).build();
        this.s3Client = S3Client.builder().region(region).build();

        // Get environment variables
        this.snsTopicArn = System.getenv("SNS_TOPIC_ARN");
        this.environment = System.getenv("ENVIRONMENT");
        this.email = System.getenv("Email");
    }

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        try {
            // Log request information
            context.getLogger().log("Received S3 event: " + s3Event);
            context.getLogger().log("Environment: " + environment);
            context.getLogger().log("TestEmail: " +  email);

            for (S3EventNotificationRecord record : s3Event.getRecords()) {
                String bucketName = record.getS3().getBucket().getName();
                String objectKey = URLDecoder.decode(record.getS3().getObject().getKey(), StandardCharsets.UTF_8.name());
                long objectSize = record.getS3().getObject().getSizeAsLong();

                context.getLogger().log("Processing file: " + objectKey + " from bucket: " + bucketName);


                // Get object metadata
                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(objectKey)
                        .build();

                GetObjectResponse objectResponse = s3Client.getObject(getObjectRequest,
                        (response, inputStream) -> response);

                String contentType = objectResponse.contentType();

                // Create message for SNS
                String message = String.format(
                        "New file uploaded to %s environment!\n\n" +
                                "File Name: %s\n" +
                                "Bucket: %s\n" +
                                "Size: %d bytes\n" +
                                "Content Type: %s\n" +
                                "Timestamp: %s",
                        environment, objectKey, bucketName, objectSize,
                        contentType, record.getEventTime()
                );

                String subject = String.format("[%s] New File Upload: %s",
                        environment.toUpperCase(), objectKey);

                // Publish message to SNS topic
                PublishRequest publishRequest = PublishRequest.builder()
                        .topicArn(snsTopicArn)
                        .subject(subject)
                        .message(message)
                        .build();

                PublishResponse publishResponse = snsClient.publish(publishRequest);
                context.getLogger().log("Message published to SNS: " + publishResponse.messageId());
            }

            return "Successfully processed S3 event";
        } catch (Exception e) {
            context.getLogger().log("Error processing S3 event: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Error processing S3 event", e);
        }
    }
}