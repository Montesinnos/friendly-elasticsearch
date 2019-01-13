# friendly-elasticsearch
Sometimes you just want ElasticSearch to work for a minute. 

## Install

```
compile "com.montesinnos.friendly:friendly-elasticsearch:0.1.2-SNAPSHOT"
```

Gradle Repository configuration
```
repositories {
    mavenCentral()
    maven {
        url 'https://oss.sonatype.org/content/repositories/snapshots/'
    }
}
```

## Description
Just want to get some ElasticSearch clusters or queries out and don't care so much about learning the specifics?
Use this library! Or even better, get this library and update it to have the best practices you know and love.

### Benefits
* Fast to get stuff done
* Easy to prototype or PC
* Ignore exceptions if that's your philosophy

## How to use

### Deployment

There's a docker installation of an ElasticSearch cluster in the /docker folder.
To run as is:
```bash
docker-compose up
```

This will deploy a 3-node cluster with 2GB of memory for each. It can be accessed at localhost:9200
Kibana will also be installed. Find it at localhost:5601


## Test
Unit tests can be run direct from the IDE (tested on IntelliJ).
Integration tests need a docker deployment. Just run the bash script `test-env.sh` in the root folder of the project
```bash
./text-even.sh

```

### Publishing
https://oss.sonatype.org/content/repositories/snapshots/com/montesinnos/lazy/friendly-elasticsearch/
```bash
gpg --export-secret-keys >~/.gnupg/secring.gpg
```
