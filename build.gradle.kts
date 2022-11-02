plugins {
  kotlin("jvm").version("1.7.20")
}

repositories {
  mavenCentral()
  maven(url = "https://packages.confluent.io/maven/")
}

dependencies {
  implementation("org.slf4j:slf4j-api:2.0.3")

  implementation("com.fasterxml.jackson.core:jackson-databind:2.13.4.2")
  implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.4")

  implementation("org.apache.beam:beam-runners-direct-java:2.42.0")
  implementation("org.apache.beam:beam-runners-google-cloud-dataflow-java:2.42.0")
  implementation("org.apache.beam:beam-sdks-java-io-google-cloud-platform:2.42.0")

  implementation("com.graphql-java:graphql-java:19.2")
}
