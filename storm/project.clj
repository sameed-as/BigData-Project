(defproject crime_storm "0.0.1-SNAPSHOT"
  :source-paths ["topologies"]
  :resource-paths ["_resources"]
  :target-path "_build"
  :min-lein-version "2.0.0"
  :jvm-opts ["-client"]
  :dependencies  [[org.apache.storm/storm-core "2.5.0"]
                  [org.apache.storm/flux-core "2.5.0"]]
  :jar-exclusions     [#"log4j\.properties" #"backtype" #"trident" #"META-INF" #"meta-inf" #"\.yaml"]
  :uberjar-exclusions [#"log4j\.properties" #"backtype" #"trident" #"META-INF" #"meta-inf" #"\.yaml"]
  :core-namespace streamparse.commands.run
  :profiles {:dev {:dependencies [[org.apache.storm/storm-core "2.5.0"]]}}
)
