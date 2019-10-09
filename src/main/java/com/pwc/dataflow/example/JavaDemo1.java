package com.pwc.dataflow.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.ParDo;

;

public class JavaDemo1 {

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
                .apply("write ", TextIO.write().to(options.getOutput()));


        pipeline.run().waitUntilFinish();
    }


    public static void main(String[] args) {
        DemoOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(DemoOptions.class);

        extractDatastore(options);
    }



}
