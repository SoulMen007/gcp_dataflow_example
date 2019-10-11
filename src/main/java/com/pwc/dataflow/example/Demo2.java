package com.pwc.dataflow.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Demo2 {

    public interface DemoOptions extends PipelineOptions {

        /** Set this required option to specify where to write the output. */
        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);
    }


    public static DatastoreV1.Read read(PipelineOptions options) {
        DatastoreV1.Read read = DatastoreIO.v1().read().withProjectId(options.as(GcpOptions.class).getProject());

        String localDatastoreHost = System.getenv("DATASTORE_EMULATOR_HOST");
        if (localDatastoreHost != null) {
            read = read.withLocalhost(localDatastoreHost);
        }

        return read;
    }

    static void extractDatastore(DemoOptions options) {

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("ReadLines",
                DatastoreIO.v1().read().withProjectId(options.as(GcpOptions.class).getProject())
                        .withLiteralGqlQuery("SELECT * FROM Payment"))
                .apply("EntityToString", ParDo.of(new EntityToString()))
                //.apply("write ", TextIO.write().to(options.getOutput()));//read the data from Datastore
                .apply(PubsubIO.writeStrings().to("projects/acuit-renfei-sandbox/topics/sam_task"));//publish to Pub/Sub
        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        DemoOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(DemoOptions.class);

        extractDatastore(options);
    }


}
