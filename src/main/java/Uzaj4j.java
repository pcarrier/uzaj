import com.google.api.client.util.Sets;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import graphql.language.*;
import graphql.parser.Parser;
import graphql.validation.DocumentVisitor;
import graphql.validation.LanguageTraversal;
import lombok.Data;
import lombok.NonNull;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

@lombok.extern.slf4j.Slf4j
public class Uzaj4j {
    public interface Options extends PipelineOptions {
    }

    @Data
    static class Interests implements Serializable {
        @JsonProperty("directives")
        private final Set<String> directives;
        @JsonProperty("directiveDefinitions")
        private final Set<String> directiveDefinitions;

        static final Interests NONE = new Interests(ImmutableSet.of(), ImmutableSet.of());

        static Interests from(Document doc) {
            final ImmutableSet.Builder<String> directives = ImmutableSet.builder();
            final ImmutableSet.Builder<String> directiveDefinitions = ImmutableSet.builder();
            new LanguageTraversal().traverse(doc, new DocumentVisitor() {
                @Override
                public void enter(Node node, List<Node> path) {
                    if (node instanceof Directive) {
                        directives.add(AstPrinter.printAstCompact(node));
                    } else if (node instanceof DirectiveDefinition) {
                        directiveDefinitions.add(AstPrinter.printAstCompact(node));
                    }
                }

                @Override
                public void leave(Node node, List<Node> path) {
                }
            });
            return new Interests(directives.build(), directiveDefinitions.build());
        }

        static final Combine.CombineFn<Interests, Interests, Interests> COMBINER =
                new Combine.CombineFn<Interests, Interests, Interests>() {
                    @Override
                    public @NonNull Interests createAccumulator() {
                        return new Interests(Sets.newHashSet(), Sets.newHashSet());
                    }

                    @Override
                    public @NonNull Interests addInput(@NonNull Interests acc, @NonNull Interests input) {
                        Objects.requireNonNull(acc).directives.addAll(Objects.requireNonNull(input).directives);
                        acc.directiveDefinitions.addAll(input.directiveDefinitions);
                        return acc;
                    }

                    @Override
                    public @NonNull Interests mergeAccumulators(@NonNull Iterable<Interests> accumulators) {
                        final Iterator<Interests> iterator = accumulators.iterator();
                        final Interests first = iterator.next();
                        while (iterator.hasNext()) {
                            addInput(first, iterator.next());
                        }
                        return first;
                    }

                    @Override
                    public @NonNull Interests extractOutput(@NonNull Interests acc) {
                        return new Interests(
                                ImmutableSet.copyOf(Objects.requireNonNull(acc).directives),
                                ImmutableSet.copyOf(acc.directiveDefinitions));
                    }
                };
    }

    private final static Parser parser = new Parser();
    private final static ObjectMapper mapper = new ObjectMapper();

    public static void main(String... args) {
        final Options options = PipelineOptionsFactory
                .fromArgs(
                        "--runner=DataflowRunner",
                        "--project=mdg-services",
                        "--region=us-central1",
                        "--workerMachineType=n1-standard-4",
                        "--numWorkers=32",
                        "--autoscalingAlgorithm=NONE",
                        "--profilingAgentConfiguration={\"APICurated\":true}")
                .withValidation()
                .as(Options.class);
        FileSystems.setDefaultPipelineOptions(options);
        final Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply("Read Spanner", SpannerIO.read()
                        .withBatching(true)
                        .withInstanceId("prod")
                        .withDatabaseId("registry")
                        .withTable("documents")
                        .withColumns("graph_id", "gzip"))
                .apply("Extract GraphQL", MapElements
                        .into(kvs(strings(), TypeDescriptor.of(Interests.class)))
                        .via((Struct input) -> {
                            final String graphID = Objects.requireNonNull(input).getString(0);
                            try (final InputStream compressed = input.getBytes(1).asInputStream();
                                 final InflaterInputStream bytes = new InflaterInputStream(compressed, new Inflater(true));
                                 final InputStreamReader text = new InputStreamReader(bytes, StandardCharsets.UTF_8);
                                 final BufferedReader buff = new BufferedReader(text)) {
                                return KV.of(graphID, Interests.from(parser.parseDocument(buff)));
                            } catch (Throwable t) {
                                log.warn("Could not extract interests", t);
                                return KV.of(graphID, Interests.NONE);
                            }
                        }))
                .apply("Combine by graph", Combine.perKey(Interests.COMBINER))
                .apply("Serialize", MapElements
                        .into(strings())
                        .via((KV<String, Interests> kv) -> {
                            try {
                                return mapper.writeValueAsString(ImmutableMap.of(
                                        "graphID", Objects.requireNonNull(Objects.requireNonNull(kv).getKey()),
                                        "interests", Objects.requireNonNull(kv.getValue())
                                ));
                            } catch (IOException e) {
                                throw new RuntimeException("Could not serialize", e);
                            }
                        }))
                .apply("Write to GCS", TextIO.write()
                        .to("gs://mdg-pcarrier-tests/uzaj4j_" + Instant.now().getEpochSecond())
                        .withSuffix(".json")
                        .withNumShards(16));
        pipeline.run().waitUntilFinish();
    }
}
