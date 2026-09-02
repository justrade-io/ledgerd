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
include("risk")
include("tests")
include("examples")
include("bench")
