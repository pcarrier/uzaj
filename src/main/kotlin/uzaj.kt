import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.cloud.spanner.PartitionOptions
import com.google.cloud.spanner.Struct
import graphql.language.*
import graphql.parser.Parser
import graphql.validation.DocumentVisitor
import graphql.validation.LanguageTraversal
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.ProcessFunction
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeDescriptors.kvs
import org.apache.beam.sdk.values.TypeDescriptors.strings
import java.io.Serializable
import java.time.Instant
import java.util.zip.InflaterInputStream

private const val workers = 3
private const val readPartitions = 64L
private const val writePartitions = 64
private val mapper: ObjectMapper = ObjectMapper()
    .registerModules(KotlinModule.Builder().build())

interface Options : PipelineOptions

data class Interests(
    val directives: Set<String>,
    val directiveDefinitions: Set<String>,
    val failed: Int,
) : Serializable {
    companion object {
        val failed = Interests(emptySet(), emptySet(), 1)
        private const val serialVersionUID = 1L
    }

    internal object Combiner : Combine.CombineFn<Interests, Combiner.Acc, Interests>() {
        data class Acc(
            val directives: MutableSet<String> = mutableSetOf(),
            val directiveDefinitions: MutableSet<String> = mutableSetOf(),
            var failed: Int = 0,
        ) : Serializable {
            companion object {
                private const val serialVersionUID = 1L
            }
        }

        override fun createAccumulator(): Acc = Acc()
        override fun addInput(acc: Acc, input: Interests) = acc.also {
            it.directives += input.directives
            it.directiveDefinitions += input.directiveDefinitions
            it.failed += input.failed
        }

        override fun mergeAccumulators(accumulators: MutableIterable<Acc>): Acc =
            accumulators.iterator().let { itr ->
                itr.next().also { first ->
                    while (itr.hasNext()) {
                        val input = itr.next()
                        first.directives += input.directives
                        first.directiveDefinitions += input.directiveDefinitions
                        first.failed += input.failed
                    }
                }
            }

        override fun extractOutput(acc: Acc) = Interests(
            directives = acc.directives,
            directiveDefinitions = acc.directiveDefinitions,
            failed = acc.failed,
        )
    }
}

internal val Document.interests: Interests
    get() {
        val directives = mutableSetOf<String>()
        val directiveDefinitions = mutableSetOf<String>()
        LanguageTraversal().traverse(this, object : DocumentVisitor {
            override fun enter(node: Node<out Node<*>>, path: List<Node<Node<*>>>) {
                when (node) {
                    is Directive -> directives += AstPrinter.printAstCompact(node)
                    is DirectiveDefinition -> directiveDefinitions += AstPrinter.printAstCompact(node)
                }
            }

            override fun leave(node: Node<out Node<*>>?, path: List<Node<Node<*>>>?) {}
        })
        return Interests(
            directives = directives,
            directiveDefinitions = directiveDefinitions,
            failed = 0,
        )
    }

fun main() {
    PipelineOptionsFactory
        .fromArgs(
            "--experiments=use_runner_v2",
            "--runner=DataflowRunner",
            "--project=mdg-services",
            "--region=us-central1",
            "--workerMachineType=e2-standard-16",
            "--numWorkers=$workers",
            "--autoscalingAlgorithm=NONE",
            "--gcpTempLocation=gs://mdg-pcarrier-tests/uzaj/tmp",
        )
        .withValidation()
        .`as`(Options::class.java)
        .let {
            FileSystems.setDefaultPipelineOptions(it)
            Pipeline.create(it)
        }
        .also { pipeline ->
            pipeline
                .apply(
                    "Read Spanner", SpannerIO
                        .read()
                        .withBatching(true)
                        .withInstanceId("prod")
                        .withDatabaseId("signatures")
                        .withTable("documents")
                        .withColumns("graph_id", "gzip")
                        .withPartitionOptions(PartitionOptions.newBuilder()
                            .setMaxPartitions(readPartitions)
                            .build())
                )
                .apply(
                    "Extract GraphQL", MapElements
                        .into(kvs(strings(), TypeDescriptor.of(Interests::class.java)))
                        .via(ProcessFunction<Struct, KV<String, Interests>> { input ->
                            val gql = input.getBytes(1)
                                .asInputStream()
                                .let { InflaterInputStream(it) }
                                .bufferedReader()
                                .readText()
                            val interests = Parser.parse(gql).interests
                            KV.of(input.getString(0), interests)
                        })
                )
                .apply(
                    "Combine by graph", Combine.perKey(Interests.Combiner)
                )
                .apply(
                    "Serialize", MapElements
                        .into(strings())
                        .via(ProcessFunction<KV<String, Interests>, String> { input ->
                            mapOf(
                                "graphID" to input.key,
                                "interests" to input.value
                            ).let { mapper.writeValueAsString(it) }
                        })
                )
                .apply(
                    "Write to GCS", TextIO.write()
                        .to("gs://mdg-pcarrier-tests/uzaj/results/${Instant.now().epochSecond}/result")
                        .withSuffix(".json")
                        .withNumShards(writePartitions)
                )
        }
        .run()
        .waitUntilFinish()
}
