package com.pwc.dataflow.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.ArrayList;
import java.util.List;

public class Demo3 {
    private static final String PAYMENT_KIND = "Payment";


    private static List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();


    static void pubSubToBigQuery(PubSubToBigQueryOptions options) {

        // Build the table schema for the output table.
        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
        fields.add(new TableFieldSchema().setName("time").setType("STRING"));
        fields.add(new TableFieldSchema().setName("message").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        Pipeline pipeline = Pipeline.create(options);


        /*
         * Steps:
         *  1) Read messages in from Pub/Sub
         *  2) Transform the PubsubMessages into TableRows
         *     - Transform message payload via UDF
         *     - Convert UDF result to TableRow objects
         *  3) Write successful records out to BigQuery
         *  4) Write failed records out to BigQuery
         */

        /*
         * Step #1: Read messages in from Pub/Sub
         * Either from a Subscription or Topic
         */

//        PCollection<PubsubMessage> messages = null;
//        if (options.getUseSubscription()) {
//            messages =
//                    pipeline.apply(
//                            "ReadPubSubSubscription",
//                            PubsubIO.readMessagesWithAttributes()
//                                    .fromSubscription(options.getInputSubscription()));
//        } else {
//            messages =
//                    pipeline.apply(
//                            "ReadPubSubTopic",
//                            PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));
//        }


        pipeline.apply(
                "ReadPubSubTopic",
                PubsubIO.readStrings().fromTopic(options.getInputTopic()))
//                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
                .apply("ConvertJsonStringToTableRow",ParDo.of(new JasonStringToTableRow()))
                .apply(
                        "WriteSuccessfulRecords",
                        BigQueryIO.writeTableRows()
                                .withSchema(schema)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .to(options.getOutputTableSpec()));//"acuit-renfei-sandbox:zuora_lin.pubsubtobigquery"

        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        PubSubToBigQueryOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToBigQueryOptions.class);

        pubSubToBigQuery(options);
    }


    public interface PubSubToBigQueryOptions extends PipelineOptions {

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Whether use the subscription")
        boolean getUseSubscription();
        void setUseSubscription(boolean value);

        @Description("Pub/sub subscription to read data from")
        String getInputSubscription();
        void setInputSubscription(String value);

        @Description("Pub/sub topic to read data from")
        @Required
        String getInputTopic();
        void setInputTopic(String value);


        @Description("Big Query Table write to")
        @Required
        String getOutputTableSpec();

        void setOutputTableSpec(String value);

        @Description("Big Query Table write to")
        int getWindowSize();
        void setWindowSize(int value);
    }


}