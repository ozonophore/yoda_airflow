import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("java")
    application
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

group = "org.cosmo"
version = System.getProperty("version") ?: "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.codeborne:selenide:7.0.4")
    implementation("org.slf4j:slf4j-api:2.0.11")
    //implementation("ch.qos.logback:logback-classic:1.2.6")
    implementation("org.slf4j:slf4j-simple:2.0.11")
}

java.targetCompatibility = JavaVersion.VERSION_17

application {
    mainClass.set("org.cosmo.Main")
}


tasks.jar {
    manifest {
        attributes(
            "Main-Class" to "org.cosmo.Main"
        )
    }
}

tasks {
    named<ShadowJar>("shadowJar") {
        archiveFileName.set("${project.name}.jar")
        mergeServiceFiles()
        manifest {
            attributes(mapOf("Main-Class" to "org.cosmo.Main"))
        }
    }
}

tasks {
    build {
        dependsOn(shadowJar)
    }
}