plugins {
  kotlin("jvm").version("1.4.21")
}

repositories {
  jcenter()
}

dependencies {
  implementation("com.fasterxml.jackson.core:jackson-databind:2.11.3")
  implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.11.3")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.11.3")

  implementation("org.apache.beam:beam-sdks-java-io-google-cloud-platform:2.26.0")
  implementation("org.apache.beam:beam-runners-google-cloud-dataflow-java:2.26.0")

  implementation("com.graphql-java:graphql-java:16.1")
}
