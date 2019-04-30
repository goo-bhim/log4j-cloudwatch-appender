package com.infiniters.aws.cloudwatch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@Plugin(name = "CloudWatchAppender", category = "Core", elementType = "appender", printObject = true)
class CloudWatchAppender extends AbstractAppender {
    private String awsAccessKey, awsSecretKey, awsRegion, awsCloudWatchGroupName, awsCloudWatchStreamName;
    private BasicAWSCredentials awsCreds;
    private AWSLogsClient cwClient;
    private Thread awsWriteThread;
    private static volatile Boolean inflight = false;
    private static volatile List<InputLogEvent> logEvents = new ArrayList();
    public CloudWatchAppender(String name, Filter filter, Layout<? extends Serializable> layout, String awsAccessKey,
                       String awsSecretKey, String awsRegion, String awsCloudWatchGroupName, String awsCloudWatchStreamName) {

        super(name, filter, layout);
        this.awsSecretKey = awsSecretKey;
        this.awsAccessKey = awsAccessKey;
        this.awsCloudWatchGroupName = awsCloudWatchGroupName;
        this.awsCloudWatchStreamName = awsCloudWatchStreamName;
        if (awsRegion == "" || awsRegion == null) {
            this.awsRegion = "us-east-1";
        } else
            this.awsRegion = awsRegion;
        System.out.println("in constructor");
        this.awsCreds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        this.cwClient = (AWSLogsClient) AWSLogsClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(this.awsCreds))
                .withRegion(awsRegion).build();
    }

    private void writeToCloud() {
        //System.out.println("in writeToCloud");
        if (!CloudWatchAppender.logEvents.isEmpty() && CloudWatchAppender.inflight == false) {
            CloudWatchAppender.inflight = true;
            PutLogEventsRequest putReq = new PutLogEventsRequest(this.awsCloudWatchGroupName, this.awsCloudWatchStreamName, logEvents)
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
                    Logger.getLogger(CloudWatchAppender.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

    }
    private String getUploadSequenceToken() {
        DescribeLogStreamsRequest req = new DescribeLogStreamsRequest().withLogGroupName(awsCloudWatchGroupName).withLogStreamNamePrefix(awsCloudWatchStreamName);
        DescribeLogStreamsResult res = this.cwClient.describeLogStreams(req);
        return res.getLogStreams().get(0).getUploadSequenceToken();
    }

    public void append(LogEvent logEvent) {
        DateFormat simple = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS Z");
        Date result = new Date(logEvent.getTimeMillis());
        byte[] bytes = getLayout().toByteArray(logEvent);
        try {
            InputLogEvent logEvent1 = new InputLogEvent().withMessage(logEvent.getLevel().name() + " : " + result + " : " + logEvent.getMessage().getFormattedMessage()).withTimestamp(System.currentTimeMillis());
            logEvents.add(logEvent1);
            this.writeToCloud();
        } catch (Exception e) {
            // TODO Auto-generated catch block
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

    @PluginFactory
    public static CloudWatchAppender createAppender(@PluginAttribute("name") String name,
                                             @PluginElement("Layout") Layout<? extends Serializable> layout,
                                             @PluginElement("Filter") final Filter filter,
                                             @PluginAttribute("awsAccessKey") String awsAccessKey,
                                             @PluginAttribute("awsSecretKey") String awsSecretKey,
                                             @PluginAttribute("awsCloudWatchGroupName") String awsCloudWatchGroupName,
                                                    @PluginAttribute("awsCloudWatchStreamName") String awsCloudWatchStreamName,
                                             @PluginAttribute("awsRegion") String awsRegion) {
        System.out.println("Initializing Cloud Watch Log4j2 Appender");
        return new CloudWatchAppender(name, filter, layout, awsAccessKey, awsSecretKey, awsRegion, awsCloudWatchGroupName, awsCloudWatchStreamName );
    }
}