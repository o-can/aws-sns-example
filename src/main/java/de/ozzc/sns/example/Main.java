package de.ozzc.sns.example;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author Ozkan Can
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);


    public static void main(@NotNull String[] args) throws InterruptedException {

        if(args.length == 0)
        {
            System.out.println("Enter a valid email address as argument");
            System.exit(-1);
        }

        final String email = args[0];

        AmazonSNSClient snsClient = new AmazonSNSClient(new DefaultAWSCredentialsProviderChain());
        snsClient.setRegion(Region.getRegion(Regions.EU_CENTRAL_1));


        CreateTopicRequest createTopicRequest = new CreateTopicRequest().withName("MyTopic");
        CreateTopicResult createTopicResult = snsClient.createTopic(createTopicRequest);
        final String topicArn = createTopicResult.getTopicArn();
        LOGGER.info("Topic ARN : "+topicArn);

        SubscribeRequest subscribeRequest =
                new SubscribeRequest()
                .withTopicArn(topicArn)
                .withProtocol("email")
                .withEndpoint(email);

        SubscribeResult subscribeResult = snsClient.subscribe(subscribeRequest);
        String subscriptionArn = subscribeResult.getSubscriptionArn();
        LOGGER.info("Subscription ARN : {}, for Endpoint {}",subscriptionArn, email);
        if(subscriptionArn.equals("confirmation pending")) {
            LOGGER.info("Subscription confirmation pending. Waiting 60s for confirmation ...");
            Thread.sleep(TimeUnit.SECONDS.toMillis(60));
        }


        String msg = "My text published to SNS topic with email endpoint";
        PublishRequest publishRequest = new PublishRequest().withMessage(msg).withTopicArn(topicArn);
        PublishResult publishResult = snsClient.publish(publishRequest);
        LOGGER.info("Message send with id {}."+publishResult.getMessageId());

        DeleteTopicRequest deleteTopicRequest = new DeleteTopicRequest().withTopicArn(topicArn);
        snsClient.deleteTopic(deleteTopicRequest);
    }
}
