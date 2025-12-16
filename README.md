## Intel® QuickAssist Technology plugin for Apache Cassandra

[Apache Cassandra](https://cassandra.apache.org/_/index.html) is a NoSQL distributed database written in Java.

The Qat-Plugin-Cassandra library improves Cassandra's compress/decompress performance by leveraging hardware acceleration provided by [Intel® QuickAssist Technology (QAT)](https://www.intel.com/content/www/us/en/products/docs/accelerator-engines/what-is-intel-qat.html).

For more details on Intel® QAT and installation instructions, refer to the [QAT Documentation](https://intel.github.io/quickassist/index.html).

### Requirements
This release was validated with the following tools and libraries:

- [QATlib v24.02.0](https://github.com/intel/qatlib)
- [QATzip v1.3.1](https://github.com/intel/QATzip/releases) and its dependencies
- [qat-java v2.3.2](https://github.com/intel/qat-java) 
- JDK 11 or later
- Clang (for fuzz testing)

### Building the source
Once all the prerequisites have been satisfied:
```
$ git clone https://github.com/intel/qat-plugin-cassandra.git
$ cd qat-plugin-cassandra
$ mvn clean package
```
### Maven targets available

- `compile` - builds sources
- `test` - builds and runs tests
- `javadoc:javadoc` - builds javadocs into ```target/site/apidocs```
- `package` - builds jar file into ```target``` directory
- `spotless:check` - check if source code is formatted well.
- `spotless:apply` - fixes source code format issues.

### Testing
To run all the unit tests:
```
mvn clean test
```
### Fuzz Testing
To enable fuzz testing, install the [Jazzer](https://github.com/CodeIntelligenceTesting/jazzer/blob/main/README.md) tool and run:
```
mvn clean test -Dfuzzing=true
```
