import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
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
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.time.Instant
import java.util.zip.Inflater
import java.util.zip.InflaterInputStream

private val log = LoggerFactory.getLogger("uzaj")
private val parser = Parser()
private val mapper: ObjectMapper = ObjectMapper().registerModules(KotlinModule())

// We currently don't need any options of our own
interface Options : PipelineOptions

data class Interests(
    val directives: Set<String>,
    val directiveDefinitions: Set<String>
) : Serializable {
    companion object {
        val none = Interests(emptySet(), emptySet())
    }

    object Combiner : Combine.CombineFn<Interests, Combiner.Acc, Interests>() {
        data class Acc(
            val directives: MutableSet<String> = mutableSetOf(),
            val directiveDefinitions: MutableSet<String> = mutableSetOf()
        ) : Serializable {
            operator fun plusAssign(input: Acc) {
                directives += input.directives
                directiveDefinitions += input.directiveDefinitions
            }

            operator fun plusAssign(input: Interests) {
                directives += input.directives
                directiveDefinitions += input.directiveDefinitions
            }
        }

        override fun createAccumulator(): Acc = Acc()
        override fun addInput(acc: Acc, input: Interests) = acc.also { it += input }

        override fun mergeAccumulators(accumulators: MutableIterable<Acc>): Acc =
            accumulators.iterator().let { itr ->
                itr.next().also { first ->
                    while (itr.hasNext()) first += itr.next()
                }
            }

        override fun extractOutput(acc: Acc) = Interests(
            directives = acc.directives,
            directiveDefinitions = acc.directiveDefinitions,
        )
    }
}

private val Document.interests: Interests
    get() {
        val directives = mutableSetOf<String>()
        val directiveDefinitions = mutableSetOf<String>()
        LanguageTraversal().traverse(this, object : DocumentVisitor {
            override fun enter(node: Node<out Node<*>>, path: List<Node<Node<*>>>) {
                when (node) {
                    is Directive -> directives += AstPrinter.printAstCompact(node)
                    is DirectiveDefinition -> directiveDefinitions += AstPrinter.printAstCompact(node)
                    else -> {
                    }
                }
            }

            override fun leave(node: Node<out Node<*>>?, path: List<Node<Node<*>>>?) {}
        })
        return Interests(
            directives = directives,
            directiveDefinitions = directiveDefinitions
        )
    }

fun main() {
    PipelineOptionsFactory
        .fromArgs(
            "--runner=DataflowRunner",
            "--project=mdg-services",
            "--region=us-central1",
            "--workerMachineType=n1-standard-4",
            "--numWorkers=32",
            "--autoscalingAlgorithm=NONE",
            "--profilingAgentConfiguration={\"APICurated\":true}"
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
                    "Read Spanner",
                    SpannerIO.read()
                        .withBatching(true)
                        .withInstanceId("prod")
                        .withDatabaseId("registry")
                        .withTable("documents")
                        // Putting the sha256 column in so the full primary is there, in case it helps sharding
                        .withColumns("graph_id", "sha256", "gzip")
                )
                .apply(
                    "Extract GraphQL",
                    MapElements.into(kvs(strings(), TypeDescriptor.of(Interests::class.java)))
                        .via(ProcessFunction<Struct, KV<String, Interests>> { input ->
                            input.getBytes(2)
                                .asInputStream()
                                .let { InflaterInputStream(it, Inflater(true)) }
                                .bufferedReader()
                                .use { buf ->
                                    KV.of(
                                        input.getString(0),
                                        try {
                                            parser.parseDocument(buf).interests
                                        } catch (t: Throwable) {
                                            Interests.none.also {
                                                log.warn("Could not extract interests", t)
                                            }
                                        }
                                    )
                                }
                        })
                )
                .apply("Combine by graph", Combine.perKey(Interests.Combiner))
                .apply(
                    "Serialize",
                    MapElements.into(strings()).via(
                        ProcessFunction<KV<String, Interests>, String> { input ->
                            mapper.writeValueAsString(
                                mapOf(
                                    "graphID" to input.key,
                                    "interests" to input.value
                                )
                            )
                        }
                    )

                )
                .apply(
                    "Write to GCS",
                    TextIO.write()
                        .to("gs://mdg-pcarrier-tests/uzaj_${Instant.now().epochSecond}")
                        .withSuffix(".json")
                        .withNumShards(16)
                )
        }
        .run()
        .waitUntilFinish()
}
