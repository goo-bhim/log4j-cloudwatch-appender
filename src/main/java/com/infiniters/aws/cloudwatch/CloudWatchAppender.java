package com.infiniters.aws.cloudwatch;

import java.io.Serializable;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.time.format.DateTimeFormatter;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.CreateLogStreamResult;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.InvalidSequenceTokenException;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

@Plugin(name = "CloudWatchAppender", category = "Core", elementType = "appender", printObject = true)
class CloudWatchAppender extends AbstractAppender {
    public String awsAccessKey, awsSecretKey, awsRegion, awsCloudWatchGroupName, awsCloudWatchStreamName, awsS3BucketName;
    private BasicAWSCredentials awsCreds;
    private AWSLogsClient cwClient;
    private AmazonS3Client s3Client;
    private Thread awsWriteThread;
    private static volatile Boolean inflight = false;
    private static volatile List<InputLogEvent> logEvents = new ArrayList();
    private static volatile List<InputLogEvent> s3Events = new ArrayList();
    private static CloudWatchAppender instance;
    public CloudWatchAppender(String name, Filter filter, Layout<? extends Serializable> layout, String awsAccessKey,
                              String awsSecretKey, String awsRegion, String awsCloudWatchGroupName, String awsCloudWatchStreamName, String awsS3BucketName) {

        super(name, filter, layout);
        this.awsSecretKey = awsSecretKey;
        this.awsAccessKey = awsAccessKey;
        this.awsCloudWatchGroupName = awsCloudWatchGroupName;
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HHmm");
        String formatDateTime = now.format(formatter);
        this.awsCloudWatchStreamName = awsCloudWatchStreamName + "-deployment-" + formatDateTime;
        this.awsS3BucketName = awsS3BucketName;
        if (awsRegion == "" || awsRegion == null) {
            this.awsRegion = "us-east-1";
        } else
            this.awsRegion = awsRegion;
        System.out.println("in constructor");
        this.awsCreds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        this.cwClient = (AWSLogsClient) AWSLogsClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(this.awsCreds))
                .withRegion(awsRegion).build();

        this.s3Client = (AmazonS3Client) AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(this.awsCreds))
                .withRegion(awsRegion)
                .build();
    }
    private synchronized  void writeToS3() {
        String stringObjKeyName = awsCloudWatchStreamName + "-" + LocalDate.now() + "/" + "payload-" + LocalDateTime.now();

        try {

            URL s3Response = null;

            // Upload a text string as a new object.
            this.s3Client.putObject(awsS3BucketName, stringObjKeyName, String.valueOf(s3Events));
            s3Response = this.s3Client.getUrl(awsS3BucketName, stringObjKeyName);
            CloudWatchAppender.s3Events.removeAll(s3Events);
            System.out.println("AWS S3 payload URL :" + s3Response);
        }
        catch(AmazonServiceException e) {
            e.printStackTrace();
        }
        catch(SdkClientException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            System.out.println("Error occurred in writeToS3");
            System.out.println("Error Message: " + e.getMessage());
            CloudWatchAppender.s3Events.removeAll(s3Events);
        }
    }

    private synchronized void writeToCloud() throws InterruptedException {

        try {


        if (!CloudWatchAppender.logEvents.isEmpty() && CloudWatchAppender.inflight == false) {
            CloudWatchAppender.inflight = true;
            Date date = new Date();
            DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
            String strDate = dateFormat.format(date);
            Thread.sleep(200);
            PutLogEventsRequest putReq = new PutLogEventsRequest(this.awsCloudWatchGroupName, this.awsCloudWatchStreamName + "-" + strDate , logEvents)
                    .withSequenceToken(this.getUploadSequenceToken());
            CloudWatchAppender.logEvents.removeAll(putReq.getLogEvents());
            this.cwClient.putLogEvents(putReq);
            CloudWatchAppender.inflight = false;

        } else {
            if (CloudWatchAppender.inflight == false) {
                try {
                    System.out.println("in else block waiting for thread to release");
                    synchronized (this.awsWriteThread) {
                        this.awsWriteThread.wait();
                    }
                } catch (InterruptedException ex) {
                    System.out.println("Error occurred in writeToCloud");
                    Logger.getLogger(CloudWatchAppender.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        } catch (Exception e) {
            System.out.println("Error occurred in writeToCloud");
            System.out.println("Error Message: " + e.getMessage());
        }

    }
    private String getUploadSequenceToken() {
        Date date = new Date();
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String strDate = dateFormat.format(date);
        DescribeLogStreamsRequest req = new DescribeLogStreamsRequest().withLogGroupName(awsCloudWatchGroupName).withLogStreamNamePrefix(awsCloudWatchStreamName + "-" + strDate);
        DescribeLogStreamsResult res = this.cwClient.describeLogStreams(req);

        if (res.getLogStreams().isEmpty()) {
            System.out.println("Creating New AWS Stream");
            CreateLogStreamRequest createReq = new CreateLogStreamRequest().withLogGroupName(awsCloudWatchGroupName).withLogStreamName(awsCloudWatchStreamName + "-" + strDate);
            CreateLogStreamResult createRes = this.cwClient.createLogStream(createReq);

            DescribeLogStreamsRequest desReq = new DescribeLogStreamsRequest().withLogGroupName(awsCloudWatchGroupName).withLogStreamNamePrefix(awsCloudWatchStreamName + "-" + strDate);
            DescribeLogStreamsResult desRes = this.cwClient.describeLogStreams(desReq);
            return desRes.getLogStreams().get(0).getUploadSequenceToken();
        } else {
            return res.getLogStreams().get(0).getUploadSequenceToken();
        }
    }

    public synchronized void append(LogEvent logEvent) {
        DateFormat simple = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS Z");
        Date result = new Date(logEvent.getTimeMillis());
        byte[] bytes = getLayout().toByteArray(logEvent);
        try {

            if (logEvent.getMessage().getFormattedMessage().toString().length() < 200000) {

                InputLogEvent logEvent1 = new InputLogEvent().withMessage(logEvent.getLevel().name() + " : " + result + " : " + logEvent.getMessage().getFormattedMessage()).withTimestamp(System.currentTimeMillis());
                logEvents.add(logEvent1);
                this.writeToCloud();

            } else {
                System.out.println("payload is too long to log, so sending it to S3");
                InputLogEvent logEvent1 = new InputLogEvent().withMessage(logEvent.getLevel().name() + " : " + result + " : " + logEvent.getMessage().getFormattedMessage().substring(0, 200000)).withTimestamp(System.currentTimeMillis());
                logEvents.add(logEvent1);
                InputLogEvent logEvent2 = new InputLogEvent().withMessage(logEvent.getLevel().name() + " : " + result + " : " + logEvent.getMessage().getFormattedMessage()).withTimestamp(System.currentTimeMillis());
                s3Events.add(logEvent2);
                this.writeToCloud();
                this.writeToS3();
            }



        } catch (InvalidSequenceTokenException invalidToken){
                System.out.println("Invalid Token Exception occurred: " + invalidToken);
                System.out.println("next expected Sequence Token: " + invalidToken.getExpectedSequenceToken());
            try {
                this.writeToCloud();
            } catch (InterruptedException e) {
                try {
                    this.writeToCloud();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                e.printStackTrace();
            }
        }
        catch (Exception e) {

            System.out.println("Error Message: " + e.getMessage());
        }
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public void setAwsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public void setAwsRegion(String awsRegion) {
        this.awsRegion = awsRegion;
    }

    public String getAwsCloudWatchGroupName() {
        return awsCloudWatchGroupName;
    }

    public void setAwsCloudWatchGroupName(String awsCloudWatchGroupName) {
        this.awsCloudWatchGroupName = awsCloudWatchGroupName;
    }

    public String getAwsCloudWatchStreamName() {
        return awsCloudWatchStreamName;
    }

    public void setAwsCloudWatchStreamName(String awsCloudWatchStreamName) {
        this.awsCloudWatchStreamName = awsCloudWatchStreamName;
    }

    public String getAwsS3BucketName() {
        return awsS3BucketName;
    }

    public void setAwsS3BucketName(String awsS3BucketName) {
        this.awsS3BucketName = awsS3BucketName;
    }

    @PluginFactory
    public static CloudWatchAppender createAppender(@PluginAttribute("name") String name,
                                                    @PluginElement("Layout") Layout<? extends Serializable> layout,
                                                    @PluginElement("Filter") final Filter filter,
                                                    @PluginAttribute("awsAccessKey") String awsAccessKey,
                                                    @PluginAttribute("awsSecretKey") String awsSecretKey,
                                                    @PluginAttribute("awsCloudWatchGroupName") String awsCloudWatchGroupName,
                                                    @PluginAttribute("awsCloudWatchStreamName") String awsCloudWatchStreamName,
                                                    @PluginAttribute("awsS3BucketName") String awsS3BucketName,
                                                    @PluginAttribute("awsRegion") String awsRegion) {
        System.out.println("Initializing Cloud Watch Log4j2 Appender");
        return new CloudWatchAppender(name, filter, layout, awsAccessKey, awsSecretKey, awsRegion, awsCloudWatchGroupName, awsCloudWatchStreamName, awsS3BucketName );
    }
}