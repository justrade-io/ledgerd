rootProject.name = "ledgerd"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("protocol")
include("core")
include("launcher")
include("write-client")
include("read")
include("read-client")
include("tests")
include("examples")
