(defproject ledgerd-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen distributed-systems verification for LEDGERD"
  :url "https://github.com/justrade-io/ledgerd"
  :license {:name "MIT"}
  :main ledgerd.core
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"]
                 ;; LEDGERD write-client transitive dependencies. The client
                 ;; classes themselves come from the Gradle-built jars listed on
                 ;; :resource-paths below; these resolve its third-party deps.
                 [io.aeron/aeron-cluster "1.48.0"]
                 [io.aeron/aeron-driver "1.48.0"]
                 [org.agrona/agrona "2.2.1"]
                 [org.hdrhistogram/HdrHistogram "2.2.2"]]
  ;; The LEDGERD write-client and protocol classes are consumed as Gradle build
  ;; outputs rather than published artifacts. Build them first from the repo root:
  ;;   ./gradlew :write-client:jar :protocol:jar
  :resource-paths ["../write-client/build/libs/write-client.jar"
                   "../protocol/build/libs/protocol.jar"]
  :jvm-opts ["--add-opens" "java.base/jdk.internal.misc=ALL-UNNAMED"
             "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED"]
  :repl-options {:init-ns ledgerd.core})
